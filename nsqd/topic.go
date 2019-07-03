package nsqd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chainhelen/dtnsq/internal/quantile"
	"github.com/chainhelen/dtnsq/internal/util"
)

type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64
	messageBytes uint64

	sync.RWMutex

	name     string
	fullName string

	channelLock sync.RWMutex
	channelMap  map[string]*Channel

	backend   BackendQueueWriter
	startChan chan int
	exitChan  chan int
	//channelUpdateChan chan int
	waitGroup util.WaitGroupWrapper
	exitFlag  int32
	idFactory *guidFactory

	ephemeral      bool
	deleteCallback func(*Topic)
	deleter        sync.Once

	paused    int32
	pauseChan chan int

	/*dtPreMessages map[MessageID]*Message
	dtPrePQ       dtPrePqueue
	dtPreMutex    sync.Mutex
	*/

	updatedBackendQueueEndChan chan bool

	innnerDt bool

	ctx *context
}

// Topic constructor
func NewTopic(topicName string, ctx *context, deleteCallback func(*Topic)) *Topic {
	t := &Topic{
		name:                       topicName,
		channelMap:                 make(map[string]*Channel),
		startChan:                  make(chan int, 1),
		exitChan:                   make(chan int),
		updatedBackendQueueEndChan: make(chan bool),
		ctx:                        ctx,
		paused:                     0,
		pauseChan:                  make(chan int),
		deleteCallback:             deleteCallback,
		idFactory:                  NewGUIDFactory(ctx.nsqd.getOpts().ID),
	}

	if strings.HasSuffix(topicName, "#ephemeral") {
		t.ephemeral = true
		t.backend = newDummyBackendQueueWriter()
	} else {
		backendName := getTopicBackendName(topicName, int(0))
		queue, err := NewDiskQueueWriter(backendName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx,
			ctx.nsqd.getOpts().SyncEvery, t.updatedBackendQueueEndChan)

		t.fullName = GetTopicFullName(topicName, int(0))

		if err != nil {
			t.ctx.nsqd.logf(LOG_ERROR, "topic(%v) failed to init disk queue: %v ", t.fullName, err)
			if err == ErrNeedFixQueueStart {
				// t.SetDataFixState(true)
			} else {
				return nil
			}
		}

		t.backend = queue
	}

	if len(topicName) > 0 && topicName[0] == '_' {
		t.innnerDt = true
	}

	t.waitGroup.Wrap(t.messagePump)
	t.ctx.nsqd.Notify(t)

	return t
}

func (t *Topic) messagePump() {
	updatedQueueChan := t.updatedBackendQueueEndChan

	for {
		if atomic.LoadInt32(&t.exitFlag) == 1 {
			goto exit
		}
		select {
		case <-updatedQueueChan:
			t.UpdatedBackendQueueEndCallback()
		case <-t.exitChan:
			break
		}
	}
exit:
	t.ctx.nsqd.logf(LOG_INFO, "Topic (%s): close ... messagePump", t.name)
}

func (t *Topic) GetChannelMapCopy() map[string]*Channel {
	tmpMap := make(map[string]*Channel)
	t.channelLock.RLock()
	for k, v := range t.channelMap {
		tmpMap[k] = v
	}
	t.channelLock.RUnlock()
	return tmpMap
}

func (t *Topic) UpdatedBackendQueueEndCallback() {
	channels := t.GetChannelMapCopy()
	for _, c := range channels {
		c.UpdateBackendQueueEnd(t.backend.GetQueueReadEnd())
	}
}

func GetTopicFullName(topic string, part int) string {
	return topic + "-" + strconv.Itoa(part)
}

func (t *Topic) Start() {
	select {
	case t.startChan <- 1:
	default:
	}
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()

	if isNew {
		// update messagePump state
		select {
		//case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		default:
			t.ctx.nsqd.logf(LOG_DEBUG, "TOPIC(%s) update channelUpdateChan %s", t.name, channel.name)
		}
	}

	return channel
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		readEnd := t.backend.GetQueueReadEnd()
		ctx := t.ctx
		channel = NewChannel(t.name, channelName, readEnd, ctx, ctx.nsqd.getOpts().MaxConfirmWin, deleteCallback)
		t.channelMap[channelName] = channel
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	numChannels := len(t.channelMap)
	t.Unlock()

	t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	channel.Delete()

	// update messagePump state
	select {
	case <-t.exitChan:
	default:
		t.ctx.nsqd.logf(LOG_DEBUG, "TOPIC(%s) update channelUpdateChan %s", t.name, channel.name)
	}

	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

// PutMessage writes a Message to the queue

func (t *Topic) PutMessage(m *Message) error {
	t.Lock()
	defer t.Unlock()

	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	_, err := t.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&t.messageCount, 1)
	atomic.AddUint64(&t.messageBytes, uint64(len(m.Body)))

	return nil
}

func (t *Topic) HandleSyncTopicFromSlave(totalMsgCnt, filenum, fileoffset, virtutaloffset, maxnum int64) ([]byte, error) {

	var (
		originTotalMsgCnt   int64
		originFilenum       int64
		originFileoffset    int64
		originVirtualoffset int64
		originMaxnum        int64
	)

	atomic.StoreInt64(&originTotalMsgCnt, totalMsgCnt)
	atomic.StoreInt64(&originFilenum, filenum)
	atomic.StoreInt64(&originFileoffset, fileoffset)
	atomic.StoreInt64(&originVirtualoffset, virtutaloffset)
	atomic.StoreInt64(&originMaxnum, maxnum)

	writer := t.backend.(*diskQueueWriter)
	leftMsgCnt := writer.diskWriteEnd.totalMsgCnt - totalMsgCnt
	needRespMsgCnt := int64(0)

	if leftMsgCnt < 0 {
		return nil, fmt.Errorf("exceed the master's totalmsgcnt")
	}

	if leftMsgCnt == 0 {

		w := bufferPoolGet()
		defer bufferPoolPut(w)

		var buf [40]byte
		binary.BigEndian.PutUint32(buf[:4], uint32(slaveSyncTopicResponseType))
		w.Write(buf[:4])
		binary.BigEndian.PutUint32(buf[:4], uint32(len([]byte(t.name))))
		w.Write(buf[:4])
		w.Write([]byte(t.name))

		binary.BigEndian.PutUint64(buf[:8], uint64(originTotalMsgCnt))
		binary.BigEndian.PutUint64(buf[8:16], uint64(originFilenum))
		binary.BigEndian.PutUint64(buf[16:24], uint64(originFileoffset))
		binary.BigEndian.PutUint64(buf[24:32], uint64(originVirtualoffset))
		binary.BigEndian.PutUint64(buf[32:], uint64(originMaxnum))
		w.Write(buf[:])

		binary.BigEndian.PutUint64(buf[:8], uint64(totalMsgCnt))
		binary.BigEndian.PutUint64(buf[8:16], uint64(filenum))
		binary.BigEndian.PutUint64(buf[16:24], uint64(fileoffset))
		binary.BigEndian.PutUint64(buf[24:32], uint64(virtutaloffset))
		binary.BigEndian.PutUint64(buf[32:], uint64(0))
		w.Write(buf[:])

		binary.BigEndian.PutUint64(buf[:8], uint64(writer.diskWriteEnd.totalMsgCnt))
		binary.BigEndian.PutUint64(buf[8:16], uint64(writer.diskWriteEnd.EndOffset.FileNum))
		binary.BigEndian.PutUint64(buf[16:24], uint64(writer.diskWriteEnd.EndOffset.Pos))
		binary.BigEndian.PutUint64(buf[24:32], uint64(writer.diskWriteEnd.virtualOffset))
		w.Write(buf[:32])

		return w.Bytes(), nil
	}

	if leftMsgCnt > maxnum {
		needRespMsgCnt = maxnum
	}

	if filenum > writer.diskWriteEnd.EndOffset.FileNum {
		return nil, fmt.Errorf("exceed the master's filenum")
	}
	curFileName := writer.fileName(filenum)
	readFile, err := os.OpenFile(curFileName, os.O_RDONLY, 0644)
	if err != nil {
		t.ctx.nsqd.logf(LOG_ERROR, "slavesynctopic (%v) open %s failed: %s", t.name, curFileName, err)
		return nil, fmt.Errorf("slavesynctopic (%v) open %s failed: %s", t.name, curFileName, err)
	}
	defer readFile.Close()
	if fileoffset > 0 {
		_, err = readFile.Seek(fileoffset, 0)
		if err != nil {
			t.ctx.nsqd.logf(LOG_ERROR, "slavesynctopic (%v) open %s, seek failed: %s", t.name, curFileName, fileoffset, err)
			return nil, fmt.Errorf("slavesynctopic (%v) open %s, seek failed: %s", t.name, curFileName, fileoffset, err)
		}
	}

	stat, err := readFile.Stat()
	if err != nil {
		t.ctx.nsqd.logf(LOG_ERROR, "slavesynctopic (%v) stat %s failed: %s", t.name, curFileName, err)
		return nil, fmt.Errorf("slavesynctopic (%v) stat %s failed: %s", t.name, curFileName, err)
	}

	if fileoffset > stat.Size() {
		atomic.AddInt64(&filenum, 1)
		atomic.StoreInt64(&fileoffset, 0)
		return t.HandleSyncTopicFromSlave(totalMsgCnt, filenum, fileoffset, virtutaloffset, maxnum)
	}

	w := bufferPoolGet()
	defer bufferPoolPut(w)

	var buf [40]byte
	binary.BigEndian.PutUint32(buf[:4], uint32(slaveSyncTopicResponseType))
	w.Write(buf[:4])
	binary.BigEndian.PutUint32(buf[:4], uint32(len([]byte(t.name))))
	w.Write(buf[:4])
	w.Write([]byte(t.name))

	binary.BigEndian.PutUint64(buf[:8], uint64(originTotalMsgCnt))
	binary.BigEndian.PutUint64(buf[8:16], uint64(originFilenum))
	binary.BigEndian.PutUint64(buf[16:24], uint64(originFileoffset))
	binary.BigEndian.PutUint64(buf[24:32], uint64(originVirtualoffset))
	binary.BigEndian.PutUint64(buf[32:], uint64(originMaxnum))
	w.Write(buf[:])

	bs := bufferPoolGet()
	for i := int64(0); i < needRespMsgCnt || fileoffset > stat.Size(); i++ {
		var (
			msgSize uint32
			buf     [4]byte
		)
		if err = binary.Read(readFile, binary.BigEndian, &msgSize); err != nil {
			bufferPoolPut(bs)
			t.ctx.nsqd.logf(LOG_ERROR, "slavesynctopic (%v) read %s,msg len failed: %s", t.name, curFileName, err)
			return nil, fmt.Errorf("slavesynctopic (%v) read %s,msg len failed: %s", t.name, curFileName, err)
		}
		data := make([]byte, msgSize)
		if err = binary.Read(readFile, binary.BigEndian, data); err != nil {
			bufferPoolPut(bs)
			t.ctx.nsqd.logf(LOG_ERROR, "slavesynctopic (%v) read %s,msg body failed: %s", t.name, curFileName, err)
			return nil, fmt.Errorf("slavesynctopic (%v) read %s,msg body failed: %s", t.name, curFileName, err)
		}

		binary.BigEndian.PutUint32(buf[:4], msgSize)
		bs.Write(buf[:])
		bs.Write(data)

		totalBytes := int64(4) + int64(msgSize)

		atomic.AddInt64(&totalMsgCnt, 1)
		atomic.AddInt64(&virtutaloffset, totalBytes)
		atomic.AddInt64(&fileoffset, totalBytes)
	}

	binary.BigEndian.PutUint64(buf[:8], uint64(totalMsgCnt))
	binary.BigEndian.PutUint64(buf[8:16], uint64(filenum))
	binary.BigEndian.PutUint64(buf[16:24], uint64(fileoffset))
	binary.BigEndian.PutUint64(buf[24:32], uint64(virtutaloffset))
	binary.BigEndian.PutUint64(buf[32:], uint64(maxnum))
	w.Write(buf[:])
	w.Write(bs.Bytes())
	bufferPoolPut(bs)

	binary.BigEndian.PutUint64(buf[:8], uint64(writer.diskWriteEnd.totalMsgCnt))
	binary.BigEndian.PutUint64(buf[8:16], uint64(writer.diskWriteEnd.EndOffset.FileNum))
	binary.BigEndian.PutUint64(buf[16:24], uint64(writer.diskWriteEnd.EndOffset.Pos))
	binary.BigEndian.PutUint64(buf[24:32], uint64(writer.diskWriteEnd.virtualOffset))
	w.Write(buf[:32])

	return w.Bytes(), nil
}

func (t *Topic) CommitDtPreMsg(msgId MessageID) {
	channel, _ := t.getOrCreateChannel("_innerDt")
	channel.CommitDtPreMsg(msgId)
}

// PutMessages writes multiple Messages to the queue
func (t *Topic) PutMessages(msgs []*Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	messageTotalBytes := 0

	for i, m := range msgs {
		_, err := t.put(m)
		if err != nil {
			atomic.AddUint64(&t.messageCount, uint64(i))
			atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
			return err
		}
		messageTotalBytes += len(m.Body)
	}

	atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	return nil
}

func (t *Topic) Flush() error {
	//TODO channel
	t.Lock()
	if err := t.backend.Flush(); err != nil {
		t.Unlock()
		return err
	}
	channels := t.GetChannelMapCopy()
	t.Unlock()

	for _, c := range channels {
		c.flush()
	}

	return nil
}

func (t *Topic) put(m *Message) (BackendQueueEnd, error) {
	b := bufferPoolGet()
	dendinfo, err := writeMessageToBackend(b, m, t.backend.Put)
	bufferPoolPut(b)
	t.ctx.nsqd.SetHealth(err)
	if err != nil {
		t.ctx.nsqd.logf(LOG_ERROR,
			"TOPIC(%s) ERROR: failed to write message to backend - %s",
			t.name, err)
	}
	m.BackendQueueEnd = dendinfo
	return dendinfo, err
}

// Delete empties the topic and all its channels and closes
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
func (t *Topic) Close() error {
	return t.exit(false)
}

func (t *Topic) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting", t.name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		t.ctx.nsqd.Notify(t)
	} else {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): closing", t.name)
	}

	close(t.exitChan)

	// synchronize the close of messagePump()
	t.waitGroup.Wait()

	if deleted {
		t.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		t.Empty()
		return t.backend.Delete()
	}

	// close all the channels
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			t.ctx.nsqd.logf(LOG_ERROR, "channel(%s) close - %s", channel.name, err)
		}
	}

	// write anything leftover to disk
	return t.backend.Close()
}

func (t *Topic) Empty() error {
	return t.backend.Empty()
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.RLock()
	realChannels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	t.RUnlock()
	for _, c := range realChannels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

func (t *Topic) Pause() error {
	return t.doPause(true)
}

func (t *Topic) UnPause() error {
	return t.doPause(false)
}

func (t *Topic) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}

	select {
	case t.pauseChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}

func (t *Topic) GenerateID() MessageID {
retry:
	id, err := t.idFactory.NewGUID()
	if err != nil {
		time.Sleep(time.Millisecond)
		goto retry
	}
	return id.Hex()
}

func (t *Topic) GetDtPreMsgByCmtMsg(msgId MessageID) (*Message, error) {
	c := t.GetChannel("_innerDt")
	return c.GetDtPreMsgByCmtMsg(msgId)
}
