package nsqd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chainhelen/dtnsq/internal/util"
	"io"
	"net"
	"sync"
	"time"
)

var FlagInconsistentError = errors.New("FlagInconsistentError")
var StepInconsistentError = errors.New("StepInconsistentError")
var DataInconsistentError = errors.New("DataInconsistentError")
var EndOfMsgVirOffsetError = errors.New("EndOfMsgVirOffsetError")

const (
	slaveSyncUnknownType         int32 = 0
	slaveSyncInfoResponseType    int32 = 100
	slaveSyncTopicResponseType   int32 = 101
	slaveSyncChannelResponseType int32 = 102
)

type Slave struct {
	sync.RWMutex
	conn               *net.TCPConn
	connMutex          sync.Mutex
	masterAddr         string
	slaveToMasterEvent chan struct{}
	ctx                *context
	//syncRequestFlag    int64
	resChan   chan Response
	exitChan  chan bool
	waitGroup util.WaitGroupWrapper
	info      map[string][]string
	infoMutex sync.RWMutex
}

type Response struct {
	frameType int32
	resType   int32
	data      []byte
	err       error
	t         string
	c         string
	info      map[string][]string
}

func NewSlave(masterAddr string, slaveToMasterEvent chan struct{}, ctx *context) *Slave {
	slave := &Slave{
		conn:               nil,
		masterAddr:         masterAddr,
		slaveToMasterEvent: slaveToMasterEvent,
		ctx:                ctx,
		resChan:            make(chan Response),
		exitChan:           make(chan bool),
	}
	slave.ConnToMaster()

	slave.waitGroup.Wrap(slave.FilterResponse)
	slave.waitGroup.Wrap(slave.HandleResponse)

	return slave
}

func (s *Slave) Close() {
	s.exit()
}

func (s *Slave) exit() {
	close(s.exitChan)
	s.ctx.nsqd.logf(LOG_INFO, "before exit, close exitChan")
	s.waitGroup.Wait()
	s.ctx.nsqd.logf(LOG_INFO, "exit...")

	s.connMutex.Lock()
	c := s.conn
	if c != nil {
		c.Close()
	}
	s.conn = nil
	s.connMutex.Unlock()
}

func (s *Slave) ConnToMaster() error {

	s.connMutex.Lock()
	defer s.connMutex.Unlock()

	if s.conn != nil {
		(*s.conn).Close()
	}

	dialer := &net.Dialer{
		//LocalAddr: localAddr,
		Timeout: time.Duration(1) * time.Second,
	}

	conn, err := dialer.Dial("tcp", s.masterAddr)
	if err != nil {
		s.ctx.nsqd.logf(LOG_ERROR, "[%s] failed to write magic - %s", s.masterAddr, err)
		return err
	}
	conn.SetDeadline(time.Now().Add(s.ctx.nsqd.getOpts().MaxHeartbeatInterval * 3))
	_, err = conn.Write(MagicDT)
	if err != nil {
		conn.Close()
		s.ctx.nsqd.logf(LOG_ERROR, "Slave connect to failed", s.masterAddr)
		return err
	}
	s.conn = conn.(*net.TCPConn)
	return nil
}

func (s *Slave) CheckConn() error {
	if s.conn == nil {
		if err := s.ConnToMaster(); err != nil {
			return err
		}
	}
	if err := s.sendToMaster([]byte("NOP\n")); err != nil {
		if err = s.ConnToMaster(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Slave) checkSyncStepType(data []byte) int32 {
	if len(data) < 4 {
		return slaveSyncUnknownType
	}
	resType := int32(binary.BigEndian.Uint32(data[:4]))
	if resType != slaveSyncInfoResponseType && resType != slaveSyncTopicResponseType &&
		resType != slaveSyncChannelResponseType {
		return slaveSyncUnknownType
	}
	return resType
}

func (s *Slave) FilterResponse() {
	for {
		select {
		case <-s.exitChan:
			return
		default:
		}

		s.connMutex.Lock()
		if s.conn == nil {
			s.connMutex.Unlock()
			continue
		}
		conn := s.conn
		s.connMutex.Unlock()

		frameType, data, err := ReadUnpackedResponse(conn)

		if err == io.EOF {
			continue
		}

		if err != nil {
			select {
			case s.resChan <- Response{data: nil, err: err, frameType: FrameTypeUnknown}:
			case <-s.exitChan:
				return
			}
		}

		if frameType == FrameTypeError {
			select {
			case s.resChan <- Response{data: data, err: nil, frameType: FrameTypeError}:
			case <-s.exitChan:
				return
			}
		}

		if frameType == FrameTypeResponse && bytes.Equal(data, []byte("_heartbeat_")) {
			if err := s.sendToMaster([]byte("NOP\n")); err != nil {
				select {
				case s.resChan <- Response{data: nil, err: err, frameType: FrameTypeUnknown}:
				case <-s.exitChan:
					return
				}
			}
		}
		if frameType == FrameTypeResponse && !bytes.Equal(data, []byte("_heartbeat_")) {
			var (
				resType int32
				res     Response
			)
			resType = s.checkSyncStepType(data)

			if resType == slaveSyncUnknownType {
				select {
				case s.resChan <- Response{
					data:      nil,
					err:       StepInconsistentError,
					frameType: FrameTypeResponse}:
				case <-s.exitChan:
					return
				}
				continue
			}
			data = data[4:]
			if resType == slaveSyncInfoResponseType {
				mp := make(map[string][]string, 0)
				if err := json.Unmarshal(data, &mp); err != nil {
					res.err = err
					res.frameType = frameTypeResponse
				} else {
					res.err = nil
					res.frameType = frameTypeResponse
					res.info = mp
					res.data = data
				}

			}
			if resType == slaveSyncTopicResponseType {
				topicSize := binary.BigEndian.Uint32(data[:4])
				data = data[4:]
				if topicSize > uint32(len(data)) {
					res.err = DataInconsistentError
				} else {
					res.t = string(data[:topicSize])
					data = data[topicSize:]
				}
			}

			if resType == slaveSyncChannelResponseType {
				topicSize := binary.BigEndian.Uint32(data[:4])
				data = data[4:]
				if topicSize > uint32(len(data)) {
					res.err = DataInconsistentError
				} else {
					res.t = string(data[:topicSize])
					data = data[topicSize:]
				}

				channelSize := binary.BigEndian.Uint32(data[:4])
				data = data[4:]
				if channelSize > uint32(len(data)) {
					res.err = DataInconsistentError
				} else {
					res.c = string(data[:channelSize])
					data = data[channelSize:]
				}
			}

			res.resType = resType
			res.data = data

			select {
			case s.resChan <- res:
			case <-s.exitChan:
				return
			}
		}
	}
}

func (s *Slave) HandleResponse() {
	for {
		select {
		case res := <-s.resChan:
			if res.err != nil {
				if res.err != FlagInconsistentError && res.err != StepInconsistentError {
					s.ctx.nsqd.logf(LOG_ERROR, "slave.HandleReponse error %s", res.err)
					s.ConnToMaster()
					continue
				}
			}
			// todo if end of file
			if res.frameType == frameTypeError {
				s.ctx.nsqd.logf(LOG_INFO, "slave.HandleReponse error %s", res.err)
				continue
			}
			if res.frameType == frameTypeResponse {
				if res.resType == slaveSyncInfoResponseType {
					s.infoMutex.Lock()
					s.info = res.info
					s.infoMutex.Unlock()
				}
				if res.resType == slaveSyncTopicResponseType {
					t := s.ctx.nsqd.GetTopic(res.t)
					if topicBackupBytes, err := s.parseSlaveSyncTopicResponse(res, t); err != nil {
						s.ctx.nsqd.logf(LOG_ERROR, "SLAVE_SYNC_TOPIC(%s) error %s", t.name, err.Error())
					} else {
						s.putBackupsIntoTopic(t, topicBackupBytes)
					}
				}
				if res.resType == slaveSyncChannelResponseType {
					t := s.ctx.nsqd.GetTopic(res.t)
					c := t.GetChannel(res.c)
					if err := s.assignmentFromSyncChannelResponse(res, c); err != nil {
						s.ctx.nsqd.logf(LOG_ERROR, "SLAVE_SYNC_CHNNAEL error %s", err.Error())
					}
				}
			}
		case <-s.exitChan:
			return
		}
	}
}

func (s *Slave) Sync() error {
	var err error
	if err = s.CheckConn(); err != nil {
		return err
	}

	if err = s.slaveSyncInfoRequest(); err != nil {
		s.ctx.nsqd.logf(LOG_ERROR, "SLAVE_SYNC_INFO error %s", err.Error())
		return err
	}

	s.infoMutex.RLock()
	defer s.infoMutex.RUnlock()

	if s.info == nil {
		return nil
	}

	for tname, chnames := range s.info {
		t := s.ctx.nsqd.GetTopic(tname)
		if err = s.slaveSyncTopicRequest(t); err != nil {
			s.ctx.nsqd.logf(LOG_ERROR, "SLAVE_SYNC_TOPIC(%s) error %s", tname, err.Error())
			break
		}

		for _, chname := range chnames {
			ch := t.GetChannel(chname)
			if err := s.slaveSyncChannelRequest(ch); err != nil {
				s.ctx.nsqd.logf(LOG_ERROR, "SLAVE_SYNC_CHNNAEL error %s", err.Error())
				break
			}
		}
	}

	return nil
}

func (s *Slave) sendToMaster(bs []byte) error {
	s.connMutex.Lock()
	defer s.connMutex.Unlock()

	(*s.conn).SetDeadline(time.Now().Add(s.ctx.nsqd.getOpts().MaxHeartbeatInterval * 3))
	if _, err := s.conn.Write(bs); err != nil {
		(*s.conn).Close()
		s.conn = nil
		return err
	}

	return nil
}

func (s *Slave) releaseSlave() {
	/*if s.Conn != nil {
		(*s.Conn).Close()
	}
	s.masterAddr = ""*/
}

// SLAVE_SYNC_INFO sync_req_bytes
func (s *Slave) slaveSyncInfoRequest() error {
	bp := bufferPoolGet()

	bp.Write([]byte("SLAVE_SYNC_INFO"))
	bp.Write([]byte("\n"))

	b := bp.Bytes()
	bufferPoolPut(bp)

	return s.sendToMaster(b)
}

// TODO need to handle error
func (s *Slave) slaveSyncTopicRequest(t *Topic) error {
	bp := bufferPoolGet()

	bp.Write([]byte("SLAVE"))
	bp.Write([]byte(" "))
	bp.Write([]byte(t.name))
	bp.Write([]byte("\n"))

	buf := make([]byte, 8)
	backend := t.backend.(*diskQueueWriter)
	backend.RLock()
	binary.BigEndian.PutUint64(buf, uint64(backend.diskWriteEnd.totalMsgCnt))
	bp.Write(buf)

	binary.BigEndian.PutUint64(buf, uint64(backend.diskWriteEnd.EndOffset.FileNum))
	bp.Write(buf)

	binary.BigEndian.PutUint64(buf, uint64(backend.diskWriteEnd.EndOffset.Pos))
	bp.Write(buf)

	binary.BigEndian.PutUint64(buf, uint64(backend.diskWriteEnd.virtualOffset))
	bp.Write(buf)

	binary.BigEndian.PutUint64(buf, uint64(10))
	bp.Write(buf)
	backend.RUnlock()

	//bp.Write([]byte("\n"))

	bs := bp.Bytes()
	bufferPoolPut(bp)

	return s.sendToMaster(bs)
}

func (s *Slave) parseSlaveSyncTopicResponse(res Response, t *Topic) ([][]byte, error) {
	backend := t.backend.(*diskQueueWriter)
	backend.RLock()
	originTotalMsgCnt := backend.diskWriteEnd.totalMsgCnt
	originFileNum := backend.diskWriteEnd.EndOffset.FileNum
	originFileOffset := backend.diskWriteEnd.EndOffset.Pos
	originVirtualOffset := backend.diskWriteEnd.virtualOffset
	backend.RUnlock()

	if res.frameType == FrameTypeResponse {
		var (
			reqTotalMsgCnt   int64
			reqFileNum       int64
			reqFileOffset    int64
			reqVirtualOffset int64
			reqMaxnum        int64

			resTotalMsgCnt   int64
			resFileNum       int64
			resFileOffset    int64
			resVirtualOffset int64
			resMaxnum        int64

			lastedTotalMsgCnt   int64
			lastedFileNum       int64
			lastedFileOffset    int64
			lastedVirtualOffset int64
		)

		data := res.data
		if data == nil {
			return nil, nil
		}

		di := int64(8)
		reqTotalMsgCnt = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		reqFileNum = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		reqFileOffset = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		reqVirtualOffset = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		reqMaxnum = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		resTotalMsgCnt = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		resFileNum = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		resFileOffset = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		resVirtualOffset = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		resMaxnum = int64(binary.BigEndian.Uint64(data[di-8 : di]))

		_ = reqMaxnum
		_ = resTotalMsgCnt
		_ = resFileNum
		_ = resFileOffset
		_ = resVirtualOffset
		_ = lastedTotalMsgCnt
		_ = lastedFileNum
		_ = lastedFileOffset

		backups := make([][]byte, resMaxnum)
		for i := int64(0); i < resMaxnum; i++ {
			di += 4
			msgSize := int64(binary.BigEndian.Uint32(data[di-4 : di]))
			di += msgSize
			backups[i] = data[di-msgSize : di]
		}

		di += 8
		lastedTotalMsgCnt = int64(binary.BigEndian.Uint64(data[di-8 : di]))

		di += 8
		lastedFileNum = int64(binary.BigEndian.Uint64(data[di-8 : di]))

		di += 8
		lastedFileOffset = int64(binary.BigEndian.Uint64(data[di-8 : di]))

		di += 8
		lastedVirtualOffset = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		_ = lastedVirtualOffset

		verified := false
		if reqTotalMsgCnt == originTotalMsgCnt && reqFileNum == originFileNum &&
			reqFileOffset == originFileOffset && reqVirtualOffset == originVirtualOffset {
			verified = true
			_ = verified
		}

		// return backups, verified, originTotalMsgCnt, lastedVirtualOffset, nil
		return backups, nil
	}
	return nil, fmt.Errorf("parseRunSyncTopicResponse unknown frameType %d", res.frameType)
}

func (slave *Slave) putBackupsIntoTopic(t *Topic, d [][]byte) {
	for i := 0; i < len(d); i++ {
		t.backend.Put(d[i])
	}

}

func (s *Slave) slaveSyncChannelRequest(c *Channel) error {
	//s.updateSyncRequestFlag()

	bp := bufferPoolGet()

	bp.Write([]byte("SLAVE"))
	bp.Write([]byte(" "))
	bp.Write([]byte(c.topicName))
	bp.Write([]byte(" "))
	bp.Write([]byte(c.name))
	//bp.Write([]byte(" "))
	//bp.Write(s.getSyncReqBytes())
	bp.Write([]byte("\n"))

	bs := bp.Bytes()
	bufferPoolPut(bp)

	return s.sendToMaster(bs)
}

func (s *Slave) assignmentFromSyncChannelResponse(res Response, c *Channel) error {
	data := res.data
	if res.frameType == FrameTypeResponse {
		backend := c.backend.(*diskQueueReader)
		backend.Lock()
		di := 8
		backend.readQueueInfo.totalMsgCnt = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		backend.readQueueInfo.EndOffset.FileNum = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		backend.readQueueInfo.EndOffset.Pos = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		backend.readQueueInfo.virtualOffset = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		backend.readQueueconfirmedInfo.totalMsgCnt = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		backend.readQueueconfirmedInfo.EndOffset.FileNum = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		backend.readQueueconfirmedInfo.EndOffset.Pos = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		backend.readQueueconfirmedInfo.virtualOffset = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		backend.readQueueEndInfo.totalMsgCnt = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		backend.readQueueEndInfo.EndOffset.FileNum = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		backend.readQueueEndInfo.EndOffset.Pos = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		di += 8
		backend.readQueueconfirmedInfo.virtualOffset = int64(binary.BigEndian.Uint64(data[di-8 : di]))
		backend.Unlock()
	}
	return fmt.Errorf("parseRunSyncTopicResponse unknown frameType %d", res.frameType)
}
