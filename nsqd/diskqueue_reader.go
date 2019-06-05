package nsqd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MAX_POSSIBLE_MSG_SIZE = 1 << 28
	readBufferSize        = 1024 * 4
)

// diskQueueReader implements the BackendQueue interface
// providing a filesystem backed FIFO queue
type diskQueueReader struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	readQueueconfirmedInfo diskQueueEndInfo
	readQueueEndInfo       diskQueueEndInfo
	readQueueInfo          diskQueueEndInfo

	// left message number for read
	depth     int64
	depthSize int64

	sync.RWMutex

	// instantiation time metadata
	readerMetaName  string
	readFrom        string
	dataPath        string
	maxBytesPerFile int64 // currently this cannot change once created
	minMsgSize      int32
	syncEvery       int64 // number of writes per fsync
	needSync        bool

	metaFlushLock sync.Mutex

	readChan chan ReadResult

	ctx *context

	readFile   *os.File
	readBuffer *bytes.Buffer

	waitingMoreData int32

	arrayExcessPosFile []*diskQueueEndInfo
}

// newDiskQueue instantiates a new instance of diskQueueReader, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func newDiskQueueReader(readFrom string, metaname string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32, ctx *context,
	syncEvery int64, syncTimeout time.Duration, readEnd BackendQueueEnd) *diskQueueReader {

	d := diskQueueReader{
		readFrom:           readFrom,
		readerMetaName:     metaname,
		dataPath:           dataPath,
		maxBytesPerFile:    maxBytesPerFile,
		minMsgSize:         minMsgSize,
		readChan:           make(chan ReadResult),
		syncEvery:          syncEvery,
		ctx:                ctx,
		readBuffer:         bytes.NewBuffer(make([]byte, 0, readBufferSize)),
		arrayExcessPosFile: make([]*diskQueueEndInfo, 0, 10),
	}

	if diskEnd, ok := readEnd.(*diskQueueEndInfo); ok {
		d.readQueueInfo = *diskEnd
		d.readQueueEndInfo = *diskEnd
	} else {
		d.ctx.nsqd.logf(LOG_ERROR, "disqueue(%s) read end not valid %v", d.readFrom, readEnd)
	}

	err := d.retrieveMetaData()
	if err != nil && !os.IsExist(err) {
		d.ctx.nsqd.logf(LOG_ERROR, "diskqueue(%s) failed to retrieveMetaData %v - %s",
			d.readFrom, d.readerMetaName, err)
	}

	return &d
}

func (d *diskQueueReader) retrieveMetaData() error {
	var (
		fileName string
		f        *os.File
		err      error
	)
	fileName = d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	var (
		cnt       int64
		fileNum   int64
		pos       int64
		virOffset int64

		ecnt       int64
		efileNum   int64
		epos       int64
		evirOffset int64
	)

	if _, err = fmt.Fscanf(f, "%d,%d,%d,%d\n%d,%d,%d,%d\n",
		&cnt, &fileNum, &pos, &virOffset, &ecnt, &efileNum, &epos, &evirOffset); err != nil {
		return err
	} else {
		d.ctx.nsqd.logf(LOG_INFO, "diskqueue(%s) reader retrieveMetaData MsgCnt:%d FileNum:%d Pos:%d Offset:%d",
			d.readerMetaName, cnt, fileNum, pos, virOffset)
	}

	atomic.StoreInt64(&d.readQueueconfirmedInfo.totalMsgCnt, cnt)
	atomic.StoreInt64(&d.readQueueconfirmedInfo.EndOffset.FileNum, fileNum)
	atomic.StoreInt64(&d.readQueueconfirmedInfo.EndOffset.Pos, pos)
	atomic.StoreInt64(&d.readQueueconfirmedInfo.virtualOffset, virOffset)

	atomic.StoreInt64(&d.readQueueInfo.totalMsgCnt, cnt)
	atomic.StoreInt64(&d.readQueueInfo.EndOffset.FileNum, fileNum)
	atomic.StoreInt64(&d.readQueueInfo.EndOffset.Pos, pos)
	atomic.StoreInt64(&d.readQueueInfo.virtualOffset, virOffset)

	atomic.StoreInt64(&d.readQueueEndInfo.totalMsgCnt, ecnt)
	atomic.StoreInt64(&d.readQueueEndInfo.EndOffset.FileNum, efileNum)
	atomic.StoreInt64(&d.readQueueEndInfo.EndOffset.Pos, epos)
	atomic.StoreInt64(&d.readQueueEndInfo.virtualOffset, evirOffset)

	return nil
}

func (d *diskQueueReader) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.reader.dat"),
		d.readerMetaName)
}

//TODO
func (d *diskQueueReader) Put([]byte) (int64, int64, error) {
	return 0, 0, nil
}

//TODO
func (d *diskQueueReader) Close() error {
	return nil
}

//TODO
func (d *diskQueueReader) Delete() error {
	return nil
}

//TODO
func (d *diskQueueReader) Empty() error {
	return nil
}

//TODO
func (d *diskQueueReader) Depth() int64 {
	return 0
}

func (d *diskQueueReader) fileName(fileNum int64) string {
	return GetQueueFileName(d.dataPath, d.readFrom, fileNum)
}

func (d *diskQueueReader) Flush() error {
	d.metaFlushLock.Lock()
	defer d.metaFlushLock.Unlock()

	var (
		f   *os.File
		err error

		cnt       int64
		fileNum   int64
		pos       int64
		virOffset int64

		ecnt       int64
		efileNum   int64
		epos       int64
		evirOffset int64
	)

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil
	}

	atomic.StoreInt64(&cnt, d.readQueueconfirmedInfo.totalMsgCnt)
	atomic.StoreInt64(&fileNum, d.readQueueconfirmedInfo.EndOffset.FileNum)
	atomic.StoreInt64(&pos, d.readQueueconfirmedInfo.EndOffset.Pos)
	atomic.StoreInt64(&virOffset, d.readQueueconfirmedInfo.virtualOffset)

	atomic.StoreInt64(&ecnt, d.readQueueEndInfo.totalMsgCnt)
	atomic.StoreInt64(&efileNum, d.readQueueEndInfo.EndOffset.FileNum)
	atomic.StoreInt64(&epos, d.readQueueEndInfo.EndOffset.Pos)
	atomic.StoreInt64(&evirOffset, d.readQueueEndInfo.virtualOffset)

	_, err = fmt.Fprintf(f, "%d,%d,%d,%d\n%d,%d,%d,%d\n", cnt, fileNum, pos, virOffset,
		ecnt, efileNum, epos, evirOffset)

	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	return os.Rename(tmpFileName, fileName)
}

func (d *diskQueueReader) TryReadOne() (*ReadResult, bool) {
	if d.readQueueEndInfo.virtualOffset <= d.readQueueInfo.virtualOffset {
		return nil, false
	}

	d.Lock()
	defer d.Unlock()

	var (
		err     error
		stat    os.FileInfo
		msgSize int32
		result  ReadResult
		bqe     diskQueueEndInfo
	)

CheckFileOpen:
	if d.readFile == nil {
		curFileName := d.fileName(d.readQueueInfo.EndOffset.FileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0644)
		if err != nil {
			result.Err = err
			d.ctx.nsqd.logf(LOG_ERROR, "diskqueuereader (%v): tryreadone failed, %s", d.readFrom, err.Error())
			return nil, false
		}
		if d.readQueueInfo.EndOffset.Pos > 0 {
			_, err = d.readFile.Seek(d.readQueueInfo.EndOffset.Pos, 0)
			if err != nil {
				result.Err = err
				d.ctx.nsqd.logf(LOG_ERROR, "diskqueuereader (%v): seek failed, %s", d.readFrom, err.Error())
				d.readFile.Close()
				d.readFile = nil
				return nil, false
			}
		}
	}
	if d.readQueueInfo.EndOffset.FileNum < d.readQueueEndInfo.EndOffset.FileNum {
		stat, err = d.readFile.Stat()
		if err != nil {
			result.Err = err
			d.ctx.nsqd.logf(LOG_ERROR, "diskqueuereader (%v): stat failed, %s", d.readFrom, err.Error())
			return nil, false
		}
		if d.readQueueInfo.EndOffset.Pos >= stat.Size() {
			atomic.AddInt64(&d.readQueueInfo.EndOffset.FileNum, 1)
			atomic.StoreInt64(&d.readQueueInfo.EndOffset.Pos, 0)

			tmpExcessInfo := &diskQueueEndInfo{}
			atomic.StoreInt64(&tmpExcessInfo.virtualOffset, d.readQueueEndInfo.virtualOffset)
			atomic.StoreInt64(&tmpExcessInfo.totalMsgCnt, d.readQueueEndInfo.totalMsgCnt)
			atomic.StoreInt64(&tmpExcessInfo.EndOffset.FileNum, d.readQueueEndInfo.EndOffset.FileNum)
			atomic.StoreInt64(&tmpExcessInfo.EndOffset.Pos, d.readQueueInfo.EndOffset.Pos)

			d.arrayExcessPosFile = append(d.arrayExcessPosFile, tmpExcessInfo)
			d.readFile.Close()
			d.readFile = nil
			goto CheckFileOpen
		}
	}

	if err = binary.Read(d.readFile, binary.BigEndian, &msgSize); err != nil {
		result.Err = err
		d.ctx.nsqd.logf(LOG_ERROR, "diskqueuereader (%v): binary.Read msgSize failed, %s", d.readFrom, err.Error())
		return nil, false
	}

	if msgSize <= 0 || msgSize > MAX_POSSIBLE_MSG_SIZE {
		err = fmt.Errorf("diskqueuereader (%v): binary.Read msgSize error, %s  msgSize:%d", d.readFrom, msgSize)
		result.Err = err
		d.ctx.nsqd.logf(LOG_ERROR, err.Error())
		return nil, false
	}

	result.Data = make([]byte, msgSize)

	if err = binary.Read(d.readFile, binary.BigEndian, result.Data); err != nil {
		result.Err = err
		d.ctx.nsqd.logf(LOG_ERROR, "diskqueuereader (%v): binary.Read result.Data failed, %s", d.readFrom, err.Error())
		return nil, false
	}

	atomic.StoreInt64(&bqe.virtualOffset, d.readQueueInfo.virtualOffset)
	bqe.totalMsgCnt = atomic.AddInt64(&d.readQueueInfo.totalMsgCnt, 1)
	atomic.StoreInt64(&bqe.EndOffset.FileNum, d.readQueueEndInfo.EndOffset.FileNum)
	atomic.StoreInt64(&bqe.EndOffset.Pos, d.readQueueEndInfo.EndOffset.Pos)

	totalBytes := int64(4) + int64(msgSize)

	d.readQueueInfo.virtualOffset += totalBytes
	d.readQueueInfo.EndOffset.Pos += totalBytes

	result.bqe = &bqe

	//TODO  这地方是个坑，因为可能造成阻塞，LOCK的地方需要注意
	// 竟然写出了一个死锁
	//select {
	//case d.readChan <- result:
	//case <-d.exitChan:
	//}

	return &result, true
}

func (d *diskQueueReader) ReadChan() chan ReadResult {
	return d.readChan
}

func (d *diskQueueReader) UpdateBackendQueueEnd(queueEndInfo BackendQueueEnd) {
	if diskEnd, ok := queueEndInfo.(*diskQueueEndInfo); ok {
		d.readQueueEndInfo = *diskEnd
	}
}

// TODO optimization
func (d *diskQueueReader) Confirm(start int64, end int64, endCnt int64) bool {
	d.RLock()
	defer d.RUnlock()

	if start <= d.readQueueconfirmedInfo.virtualOffset {
		if end <= d.readQueueconfirmedInfo.virtualOffset {
			return true
		}
		d.readQueueconfirmedInfo.totalMsgCnt = endCnt
		d.readQueueconfirmedInfo.virtualOffset = end

		for i := 0; i < len(d.arrayExcessPosFile); i++ {
			if end > d.arrayExcessPosFile[i].virtualOffset {
				d.readQueueconfirmedInfo.EndOffset.FileNum = d.arrayExcessPosFile[i].EndOffset.FileNum
				d.readQueueconfirmedInfo.EndOffset.Pos = d.arrayExcessPosFile[i].EndOffset.Pos
			}
		}
		return true
	}

	return false
}
