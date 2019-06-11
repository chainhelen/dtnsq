package nsqd

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"github.com/chainhelen/dtnsq/internal/util"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chainhelen/dtnsq/internal/pqueue"
	"github.com/chainhelen/dtnsq/internal/quantile"
)

type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats() ClientStats
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64
	messageCount uint64
	timeoutCount uint64

	sync.RWMutex

	topicName string
	name      string
	ctx       *context

	backend BackendQueueReader

	//memoryMsgChan chan *Message
	clientMsgChan       chan *Message
	clientCheckBackChan chan *Message
	exitChan            chan int
	exitFlag            int32
	exitMutex           sync.RWMutex

	confirmedMsgs             *IntervalTree
	updateBackendQueueEndChan chan bool
	tryReadOneChan            chan bool

	// state tracking
	clients        map[int64]Consumer
	paused         int32
	ephemeral      bool
	deleteCallback func(*Channel)
	deleter        sync.Once
	innerDt        bool

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	// TODO: these can be DRYd up
	deferredMessages map[MessageID]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue
	deferredMutex    sync.Mutex
	inFlightMessages map[MessageID]*Message
	inFlightPQ       inFlightPqueue
	inFlightMutex    sync.Mutex
	dtPreMessages    map[MessageID]*Message
	dtPrePQ          dtPrePqueue
	dtPreMutex       sync.Mutex

	waitGroup util.WaitGroupWrapper

	maxWin int64
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, channelName string, chEnd BackendQueueEnd,
	ctx *context, maxWin int64, deleteCallback func(*Channel)) *Channel {

	c := &Channel{
		topicName:                 topicName,
		name:                      channelName,
		clientMsgChan:             make(chan *Message),
		clientCheckBackChan:       make(chan *Message),
		exitChan:                  make(chan int),
		clients:                   make(map[int64]Consumer),
		confirmedMsgs:             NewIntervalTree(),
		updateBackendQueueEndChan: make(chan bool),
		tryReadOneChan:            make(chan bool, 1),
		deleteCallback:            deleteCallback,
		ctx:                       ctx,
		maxWin:                    maxWin,
	}
	if len(ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
			ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}

	c.initPQ()

	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeral = true
		c.backend = newDummyBackendQueueReader()
	} else {
		// backend names, for uniqueness, automatically include the topic...
		// TODO
		backendReaderName := getBackendReaderName(c.topicName, 0, channelName)
		backendWriterName := getBackendWriterName(c.topicName, 0)

		c.backend = newDiskQueueReader(
			backendWriterName,
			backendReaderName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			chEnd,
		)
	}

	if len(channelName) > 0 && channelName[0] == '_' {
		c.innerDt = true
	}

	c.waitGroup.Wrap(c.messagePump)
	c.ctx.nsqd.Notify(c)

	return c
}

func (c *Channel) messagePump() {
	maxWin := c.maxWin

LOOP:
	for {
		if atomic.LoadInt32(&c.exitFlag) == 1 {
			goto exit
		}
		if c.confirmedMsgs.Len() > maxWin {
			c.ctx.nsqd.logf(LOG_WARN, "channel (%v): len(confirmedMsg)=%d excess the limit win %d", c.confirmedMsgs.Len(), maxWin)
			continue
		}

		select {
		case <-c.exitChan:
			goto exit
		case <-c.updateBackendQueueEndChan:
			if r, ok := c.tryReadOne(); ok {
				if err := c.handleMsg(*r); err != nil {
					continue LOOP
				}
			}
		case <-c.tryReadOneChan:
			if r, ok := c.tryReadOne(); ok {
				if err := c.handleMsg(*r); err != nil {
					continue LOOP
				}
			}
		}
	}
exit:
	c.updateBackendQueueEndChan = nil
	c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): closing ... messagePump", c.name)
	close(c.clientMsgChan)
}

func (c *Channel) handleMsg(data ReadResult) error {
	if data.Err != nil {
		c.ctx.nsqd.logf(LOG_ERROR, "channel (%v): failed to read message - %s", c.name, data.Err)
		time.Sleep(time.Millisecond * 100)
		return fmt.Errorf("handleMsg failed")
	}
	msg, err := decodeMessage(data.Data)
	if err != nil {
		c.ctx.nsqd.logf(LOG_ERROR, "channel (%v): failed to read message - %s", c.name, err.Error())
		time.Sleep(time.Millisecond * 100)
		return fmt.Errorf("handleMsg failed")
	}
	msg.BackendQueueEnd = data.bqe

	if c.innerDt {
		c.StartPreDtMsgTimeout(msg, c.ctx.nsqd.getOpts().DtCheckBackTimeout)
	} else {
		select {
		case c.clientMsgChan <- msg:
		case <-c.exitChan:
		}
	}
	return nil
}

func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.ctx.nsqd.getOpts().MemQueueSize)/10))

	c.inFlightMutex.Lock()
	c.inFlightMessages = make(map[MessageID]*Message)
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()

	c.dtPreMutex.Lock()
	c.dtPreMessages = make(map[MessageID]*Message)
	c.dtPrePQ = newDtPrePqueue(pqSize)
	c.dtPreMutex.Unlock()

	c.deferredMutex.Lock()
	c.deferredMessages = make(map[MessageID]*pqueue.Item)
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

// Exiting returns a boolean indicating if this channel is closed/exiting
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// Delete empties the channel and closes
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
func (c *Channel) Close() error {
	return c.exit(false)
}

func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()

	close(c.exitChan)
	c.waitGroup.Wait()

	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): deleting", c.name)

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		c.ctx.nsqd.Notify(c)
	} else {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): closing", c.name)
	}

	// this forceably closes client connections
	c.RLock()
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()

	if deleted {
		// empty the queue (deletes the backend files, too)
		c.Empty()
		return c.backend.Delete()
	}

	// write anything leftover to disk
	c.flush()
	return c.backend.Close()
}

func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	c.initPQ()
	for _, client := range c.clients {
		client.Empty()
	}

	for {
		select {
		//case <-c.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return c.backend.Empty()
}

// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight/deferred because it is only called in Close()
func (c *Channel) flush() error {
	var msgBuf bytes.Buffer

	//if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
	//	c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
	//		c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	//}

	for {
		select {
		//case msg := <-c.memoryMsgChan:
		//	err := writeMessageToBackend(&msgBuf, msg, c.backend)
		//	if err != nil {
		////		c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		//	}
		default:
			goto finish
		}
	}

finish:
	c.inFlightMutex.Lock()
	for _, msg := range c.inFlightMessages {
		_, err := writeMessageToBackend(&msgBuf, msg, c.backend.Put)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		_, err := writeMessageToBackend(&msgBuf, msg, c.backend.Put)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.deferredMutex.Unlock()

	if err := c.backend.Flush(); err != nil {
		c.ctx.nsqd.logf(LOG_ERROR, "failed to backend  flush - %s", err)
	}

	return nil
}

func (c *Channel) Depth() int64 {
	return int64(len(c.clientMsgChan)) + c.backend.Depth()
}

func (c *Channel) Pause() error {
	return c.doPause(true)
}

func (c *Channel) UnPause() error {
	return c.doPause(false)
}

func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	c.RLock()
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}

func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage writes a Message to the queue
func (c *Channel) PutMessage(m *Message) error {
	c.RLock()
	defer c.RUnlock()
	if c.Exiting() {
		return errors.New("exiting")
	}
	err := c.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}

func (c *Channel) put(m *Message) error {
	select {
	case c.clientMsgChan <- m:
		/*default:
		b := bufferPoolGet()
		err := writeMessageToBackend(b, m, c.backend)
		bufferPoolPut(b)
		c.ctx.nsqd.SetHealth(err)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "CHANNEL(%s): failed to write message to backend - %s",
				c.name, err)
			return err
		}*/
	}
	return nil
}

func (c *Channel) PutMessageDeferred(msg *Message, timeout time.Duration) {
	atomic.AddUint64(&c.messageCount, 1)
	c.StartDeferredTimeout(msg, timeout)
}

// TouchMessage resets the timeout for an in-flight message
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)

	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.ctx.nsqd.getOpts().MaxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.ctx.nsqd.getOpts().MaxMsgTimeout)
	}

	msg.pri = newTimeout.UnixNano()
	err = c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// FinishMessage successfully discards an in-flight message
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}

	mergedInterval := c.confirmedMsgs.AddOrMerge(&QueueInterval{start: int64(msg.Offset()),
		end:    int64(msg.Offset()) + int64(msg.MovedSize),
		endCnt: int64(msg.TotalMsgCnt()),
	})

	if delete := c.backend.Confirm(mergedInterval.Start(), mergedInterval.End(), mergedInterval.EndCnt()); delete {
		c.confirmedMsgs.DeleteInterval(mergedInterval)
	}

	return nil
}

func (c *Channel) CommitDtPreMsg(msgId MessageID) error {
	msg, err := c.popDtPreMessage(msgId)
	if err != nil {
		return err
	}
	c.removeFromDtPrePQ(msg)

	mergedInterval := c.confirmedMsgs.AddOrMerge(&QueueInterval{start: int64(msg.Offset()),
		end:    int64(msg.Offset()) + int64(msg.MovedSize),
		endCnt: int64(msg.TotalMsgCnt()),
	})

	if delete := c.backend.Confirm(mergedInterval.Start(), mergedInterval.End(), mergedInterval.EndCnt()); delete {
		c.confirmedMsgs.DeleteInterval(mergedInterval)
	}

	return nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	// remove from inflight first
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	atomic.AddUint64(&c.requeueCount, 1)

	if timeout == 0 {
		c.exitMutex.RLock()
		if c.Exiting() {
			c.exitMutex.RUnlock()
			return errors.New("exiting")
		}
		err := c.put(msg)
		c.exitMutex.RUnlock()
		return err
	}

	// deferred requeue
	return c.StartDeferredTimeout(msg, timeout)
}

// AddClient adds a client to the Channel's client list
func (c *Channel) AddClient(clientID int64, client Consumer) error {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if ok {
		return nil
	}

	maxChannelConsumers := c.ctx.nsqd.getOpts().MaxChannelConsumers
	if maxChannelConsumers != 0 && len(c.clients) >= maxChannelConsumers {
		return errors.New("E_TOO_MANY_CHANNEL_CONSUMERS")
	}

	c.clients[clientID] = client
	return nil
}

// RemoveClient removes a client from the Channel's client list
func (c *Channel) RemoveClient(clientID int64) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	delete(c.clients, clientID)

	if len(c.clients) == 0 && c.ephemeral == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

func (c *Channel) StartPreDtMsgTimeout(msg *Message, timeout time.Duration) error {
	now := time.Now()
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	err := c.pushDtPreMessage(msg)
	if err != nil {
		return err
	}
	c.addToDtPrePQ(msg)
	return nil
}

func (c *Channel) RestartPreDtMsgTimeout(msg *Message, timeout time.Duration) {
	now := time.Now()
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	c.pushDtPreMessage(msg)
	c.addToDtPrePQ(msg)
}

func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	absTs := time.Now().Add(timeout).UnixNano()
	item := &pqueue.Item{Value: msg, Priority: absTs}
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	c.addToDeferredPQ(item)
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	delete(c.inFlightMessages, id)
	c.inFlightMutex.Unlock()
	return msg, nil
}

func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}

// pushDtPreMessage atomically adds a message to the dtpre dictionary
func (c *Channel) pushDtPreMessage(msg *Message) error {
	c.dtPreMutex.Lock()
	_, ok := c.dtPreMessages[msg.ID]
	if ok {
		c.dtPreMutex.Unlock()
		return errors.New("ID already in dtpre")
	}
	c.dtPreMessages[msg.ID] = msg
	c.dtPreMutex.Unlock()
	return nil
}

// popDtpreMessage atomically removes a message from the dtpre dictionary
func (c *Channel) popDtPreMessage(id MessageID) (*Message, error) {
	c.dtPreMutex.Lock()
	msg, ok := c.dtPreMessages[id]
	if !ok {
		c.dtPreMutex.Unlock()
		return nil, errors.New("ID not in dtpre")
	}
	delete(c.dtPreMessages, id)
	c.dtPreMutex.Unlock()
	return msg, nil
}

func (c *Channel) GetDtPreMsgByCmtMsg(msgId MessageID) (*Message, error) {
	c.dtPreMutex.Lock()
	msg, ok := c.dtPreMessages[msgId]
	if !ok {
		c.dtPreMutex.Unlock()
		return nil, errors.New("ID not in dtpre")
	}
	c.dtPreMutex.Unlock()
	return msg, nil
}

func (c *Channel) addToDtPrePQ(msg *Message) {
	c.dtPreMutex.Lock()
	c.dtPrePQ.Push(msg)
	c.dtPreMutex.Unlock()
}

func (c *Channel) removeFromDtPrePQ(msg *Message) {
	c.dtPreMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.dtPreMutex.Unlock()
		return
	}
	c.dtPrePQ.Remove(msg.index)
	c.dtPreMutex.Unlock()
}

func (c *Channel) ListMost10Item() []*Message {
	c.dtPreMutex.Lock()
	arr := c.dtPrePQ.ListMost10Item()
	c.dtPreMutex.Unlock()
	return arr
}

func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	id := item.Value.(*Message).ID
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item
	c.deferredMutex.Unlock()
	return nil
}

func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)
	c.deferredMutex.Unlock()
	return item, nil
}

func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}

func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.deferredMutex.Lock()
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true

		msg := item.Value.(*Message)
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}
		c.put(msg)
	}

exit:
	return dirty
}

func (c *Channel) tryReadOne() (*ReadResult, bool) {
	return c.backend.TryReadOne()
}

func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		dirty = true

		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}
		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			client.TimedOutMessage()
		}
		c.put(msg)
	}

exit:
	return dirty
}

func (c *Channel) processDtPreQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	tmpMsg := make([]*Message, 0, len(c.dtPrePQ))
	for {
		c.dtPreMutex.Lock()
		msg, _ := c.dtPrePQ.PeekAndShift(t)
		c.dtPreMutex.Unlock()

		// c.ctx.nsqd.logf(LOG_ERROR, "processDtPreQueue %s", msg)

		if msg == nil {
			goto exit
		}
		tmpMsg = append(tmpMsg, msg)
		dirty = true

		_, err := c.GetDtPreMsgByCmtMsg(msg.ID)
		if err != nil {
			goto exit
		}

		// c.put(msg)
		if t := c.ctx.nsqd.GetTopic(c.topicName); t == nil {
			c.ctx.nsqd.logf(LOG_ERROR, "processDtPreQueue can't find topicName for topic:%s", c.topicName)
			goto exit
		} else {
			c.ctx.nsqd.logf(LOG_DEBUG, "processDtPreQueue msgId:%s Body:%s topic:%s", msg.ID, msg.Body, t.name)
			c.CheckBack(msg)
		}
	}

exit:

	for i := 0; i < len(tmpMsg); i++ {
		c.RestartPreDtMsgTimeout(tmpMsg[i], c.ctx.nsqd.getOpts().DtCheckBackTimeout)
	}

	return dirty
}

func (c *Channel) CheckBack(m *Message) {
	c.RLock()
	defer c.RUnlock()

	if 0 == len(c.clients) {
		c.ctx.nsqd.logf(LOG_DEBUG, "TOPIC(%s) CHANNEL(%s): before send premsg checkBack failed, len(c.clients)=0", c.topicName, c.name)
		return
	}

	c.ctx.nsqd.logf(LOG_DEBUG, "TOPIC(%s) CHANNEL(%s): before send premsg checkBack", c.topicName, c.name)

	select {
	case c.clientCheckBackChan <- m:
	case <-c.exitChan:
	}
}

func (c *Channel) UpdateBackendQueueEnd(backendQueueEnd BackendQueueEnd) {
	c.backend.UpdateBackendQueueEnd(backendQueueEnd)
	if nil != c.updateBackendQueueEndChan {
		c.updateBackendQueueEndChan <- true
	}
}
