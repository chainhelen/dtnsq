package nsqd

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

const (
	MAX_QUEUE_OFFSET_META_DATA_KEEP = 100
)

var (
	ErrInvalidOffset     = errors.New("invalid offset")
	ErrNeedFixQueueStart = errors.New("init queue start should be fixed")
	writeBufSize         = 1024 * 128
)

type diskQueueWriter struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	// run-time state (also persisted to disk)
	diskWriteEnd diskQueueEndInfo
	diskReadEnd  diskQueueEndInfo
	// the start of the queue , will be set to the cleaned offset
	// diskQueueStart diskQueueEndInfo
	sync.RWMutex

	// instantiation time metadata
	name            string
	dataPath        string
	maxBytesPerFile int64 // currently this cannot change once created
	minMsgSize      int32
	maxMsgSize      int32
	exitFlag        int32
	exitChan        chan bool
	needSync        bool

	updatedBackendQueueEndChan chan bool

	ctx *context

	writeFile    *os.File
	bufferWriter *bufio.Writer
	bufSize      int64

	metaFlushLock sync.RWMutex
}

func NewDiskQueueWriter(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32, ctx *context,
	syncEvery int64, updatedBackendQueueEndChan chan bool) (BackendQueueWriter, error) {
	return newDiskQueueWriter(name, dataPath, maxBytesPerFile,
		minMsgSize, maxMsgSize, ctx, syncEvery, false, updatedBackendQueueEndChan)
}

// newDiskQueue instantiates a new instance of diskQueueWriter, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func newDiskQueueWriter(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32, ctx *context,
	syncEvery int64, readOnly bool, updatedBackendQueueEndChan chan bool) (BackendQueueWriter, error) {

	d := diskQueueWriter{
		name:                       name,
		dataPath:                   dataPath,
		maxBytesPerFile:            maxBytesPerFile,
		minMsgSize:                 minMsgSize,
		maxMsgSize:                 maxMsgSize,
		exitChan:                   make(chan bool),
		updatedBackendQueueEndChan: updatedBackendQueueEndChan,
		ctx:                        ctx,
		needSync:                   true,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.ctx.nsqd.logf(LOG_ERROR, "diskqueue(%s) failed to retrieveMetaData - %s", d.name, err)
		return &d, err
	}

	return &d, nil
}

func (d *diskQueueWriter) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
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
	)
	if _, err = fmt.Fscanf(f, "%d\n%d,%d,%d\n",
		&cnt, &fileNum, &pos, &virOffset); err != nil {
		return err
	} else {
		d.ctx.nsqd.logf(LOG_INFO, "diskqueue(%s) writer retrieveMetaData MsgCnt:%d FileNum:%d Pos:%d Offset:%d",
			d.name, cnt, fileNum, pos, virOffset)
	}

	atomic.StoreInt64(&d.diskWriteEnd.totalMsgCnt, cnt)
	atomic.StoreInt64(&d.diskWriteEnd.EndOffset.FileNum, fileNum)
	atomic.StoreInt64(&d.diskWriteEnd.EndOffset.Pos, pos)
	atomic.StoreInt64(&d.diskWriteEnd.virtualOffset, virOffset)

	d.diskReadEnd = d.diskWriteEnd

	return nil
}

func (d *diskQueueWriter) writeOne(data []byte) (int64, *diskQueueEndInfo, error) {
	var err error

	if d.writeFile == nil {
		curFileName := d.fileName(d.diskWriteEnd.EndOffset.FileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return 0, nil, err
		}
		d.ctx.nsqd.logf(LOG_INFO, "DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.diskWriteEnd.EndOffset.Pos > 0 {
			_, err = d.writeFile.Seek(d.diskWriteEnd.EndOffset.Pos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return 0, nil, err
			}
		}

		if d.bufferWriter == nil {
			d.bufferWriter = bufio.NewWriterSize(d.writeFile, writeBufSize)
		} else {
			d.bufferWriter.Reset(d.writeFile)
		}
	}

	d.needSync = true
	dataLen := int32(len(data))

	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return 0, nil, fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	writeOffset := d.diskWriteEnd.virtualOffset
	totalBytes := int64(dataLen) + 4

	d.diskWriteEnd.EndOffset.Pos += totalBytes
	d.diskWriteEnd.virtualOffset += totalBytes
	atomic.AddInt64(&d.diskWriteEnd.totalMsgCnt, 1)

	err = binary.Write(d.bufferWriter, binary.BigEndian, dataLen)
	if err != nil {
		d.sync(true, false)
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.ctx.nsqd.logf(LOG_INFO, "DISKQUEUE(%s): writeOne() faled %s", d.name, err)
		return 0, nil, err
	}

	_, err = d.bufferWriter.Write(data)
	if err != nil {
		d.sync(true, false)
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.ctx.nsqd.logf(LOG_INFO, "DISKQUEUE(%s): writeOne() faled %s", d.name, err)
		return 0, nil, err
	}

	if d.diskWriteEnd.EndOffset.Pos >= d.maxBytesPerFile {
		// sync every time we start writing to a new file
		err = d.sync(true, false)
		if err != nil {
			d.ctx.nsqd.logf(LOG_ERROR, "diskqueue(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.ctx.nsqd.logf(LOG_INFO, "DISKQUEUE(%s): new file write, old file: filenum:%d,pos:%d,virtualOffset:%d,totalMsgCnt:%d",
			d.name, d.diskWriteEnd.EndOffset.FileNum, d.diskWriteEnd.EndOffset.Pos, d.diskWriteEnd.virtualOffset, d.diskWriteEnd.totalMsgCnt)

		d.diskWriteEnd.EndOffset.FileNum++
		d.diskWriteEnd.EndOffset.Pos = 0

		d.ctx.nsqd.logf(LOG_INFO, "DISKQUEUE(%s): new file write, new file: filenum:%d,pos:%d,virtualOffset:%d,totalMsgCnt:%d",
			d.name, d.diskWriteEnd.EndOffset.FileNum, d.diskWriteEnd.EndOffset.Pos, d.diskWriteEnd.virtualOffset, d.diskWriteEnd.totalMsgCnt)

		d.diskReadEnd = d.diskWriteEnd
	}

	return writeOffset, nil, err
}

func (d *diskQueueWriter) sync(fsync bool, metaSync bool) error {
	if d.needSync == false {
		return nil
	}

	if d.bufferWriter != nil {
		d.bufferWriter.Flush()
		d.ctx.nsqd.logf(LOG_DEBUG, "DISKQUEUE(%s) bufferWriter flush into filename:%s", d.name, d.fileName(d.diskWriteEnd.EndOffset.FileNum))
	}
	if d.writeFile != nil && fsync {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	d.diskReadEnd = d.diskWriteEnd

	select {
	case d.updatedBackendQueueEndChan <- true:
	case <-d.exitChan:
	}

	if metaSync {
		err := d.persistMetaData(d.diskWriteEnd)
		if err != nil {
			return err
		}
		d.ctx.nsqd.logf(LOG_DEBUG, "DISKQUEUE(%s) persistMetaData filename:%s", d.name, d.metaDataFileName())
	}

	if fsync && metaSync {
		d.needSync = false
	}

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueueWriter) persistMetaData(writeEnd diskQueueEndInfo) error {
	d.metaFlushLock.Lock()
	defer d.metaFlushLock.Unlock()

	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	//_, err = fmt.Fprintf(f, "%d\n%d,%d,%d\n",
	//	writeEnd.totalMsgCnt,
	//	writeEnd.EndOffset.FileNum, writeEnd.EndOffset.Pos, writeEnd.Offset())

	_, err = fmt.Fprintf(f, "%d\n%d,%d,%d\n",
		writeEnd.totalMsgCnt,
		writeEnd.EndOffset.FileNum, writeEnd.EndOffset.Pos, writeEnd.Offset())

	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (d *diskQueueWriter) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.writer.dat"), d.name)
}

func (d *diskQueueWriter) fileName(fileNum int64) string {
	return GetQueueFileName(d.dataPath, d.name, fileNum)
}

func GetQueueFileName(dataRoot string, base string, fileNum int64) string {
	return fmt.Sprintf(path.Join(dataRoot, "%s.diskqueue.%09d.dat"), base, fileNum)
}

func (d *diskQueueWriter) Put(data []byte) (int64, int64, error) {
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return 0, 0, errors.New("exiting")
	}

	offset, dendinfo, werr := d.writeOne(data)

	if dendinfo == nil {
		dendinfo = &diskQueueEndInfo{}
	}

	return offset, dendinfo.TotalMsgCnt(), werr
}

func (d *diskQueueWriter) Flush() error {
	d.Lock()
	defer d.Unlock()

	if d.needSync == false {
		return nil
	}

	return d.syncAll()
}

// Close cleans up the queue and persists metadata
func (d *diskQueueWriter) Close() error {
	close(d.exitChan)
	return d.exit(false)
}

func (d *diskQueueWriter) Delete() error {
	return d.exit(true)
}

func (d *diskQueueWriter) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1

	if deleted {
		d.ctx.nsqd.logf(LOG_INFO, "DISKQUEUE(%s): deleting", d.name)
	} else {
		d.ctx.nsqd.logf(LOG_INFO, "DISKQUEUE(%s): closing", d.name)
	}

	d.syncAll()
	if deleted {
		return d.deleteAllFiles(deleted)
	}

	return nil
}

// sync fsyncs the current writeFile and persists metadata
func (d *diskQueueWriter) syncAll() error {
	return d.sync(true, true)
}

func (d *diskQueueWriter) deleteAllFiles(deleted bool) error {
	d.cleanOldData()
	d.persistMetaData(d.diskWriteEnd)

	if deleted {
		d.ctx.nsqd.logf(LOG_INFO, "DISKQUEUE(%s): deleting meta file", d.name)
		innerErr := os.Remove(d.metaDataFileName())
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.ctx.nsqd.logf(LOG_ERROR, "diskqueue(%s) failed to remove metadata file - %s", d.name, innerErr)
			return innerErr
		}
	}
	return nil
}

func (d *diskQueueWriter) cleanOldData() error {
	// TODO close current file
	cleanStartFileNum := d.diskReadEnd.EndOffset.FileNum - MAX_QUEUE_OFFSET_META_DATA_KEEP - 1
	if cleanStartFileNum < 0 {
		cleanStartFileNum = 0
	}
	for i := cleanStartFileNum; i <= d.diskWriteEnd.EndOffset.FileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		d.ctx.nsqd.logf(LOG_DEBUG, "DISKQUEUE(%s): removed data file: %v", d.name, fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.ctx.nsqd.logf(LOG_ERROR, "diskqueue(%s) failed to remove data file - %s", d.name, innerErr)
		} else {
			fName := d.fileName(i) + ".offsetmeta.dat"
			innerErr := os.Remove(fName)
			d.ctx.nsqd.logf(LOG_INFO, "DISKQUEUE(%s): removed offset meta file: %v", d.name, fName)
			if innerErr != nil && !os.IsNotExist(innerErr) {
				d.ctx.nsqd.logf(LOG_ERROR, "diskqueue(%s) failed to remove offset meta file %v - %s", d.name, fName, innerErr)
			}
		}
	}

	d.diskWriteEnd.EndOffset.FileNum++
	d.diskWriteEnd.EndOffset.Pos = 0
	d.diskReadEnd = d.diskWriteEnd
	// d.diskQueueStart = d.diskWriteEnd
	return nil
}

func (d *diskQueueWriter) Empty() error {
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.ctx.nsqd.logf(LOG_INFO, "DISKQUEUE(%s): emptying", d.name)
	return d.deleteAllFiles(false)
}

func (d *diskQueueWriter) EndInfo() {

}

func (d *diskQueueWriter) GetQueueReadEnd() BackendQueueEnd {
	e := &diskQueueEndInfo{}
	*e = d.diskReadEnd
	return e
}

func (d *diskQueueWriter) GetQueueCurWriterEnd() BackendQueueEnd {
	e := &diskQueueEndInfo{}
	*e = d.diskWriteEnd
	return e
}
