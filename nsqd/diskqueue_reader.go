package nsqd

import (
	"bytes"
	"os"
	"sync"
	"time"
)

const (
	readBufferSize = 1024 * 4
)

// diskQueueReader implements the BackendQueue interface
// providing a filesystem backed FIFO queue
type diskQueueReader struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	readQueueInfo diskQueueEndInfo
	queueEndInfo  diskQueueEndInfo
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
	exitFlag        int32
	needSync        bool

	confirmedQueueInfo diskQueueEndInfo

	ctx *context

	readFile   *os.File
	readBuffer *bytes.Buffer

	exitChan        chan int
	waitingMoreData int32
}

// newDiskQueue instantiates a new instance of diskQueueReader, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func newDiskQueueReader(readFrom string, metaname string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32, ctx *context,
	syncEvery int64, syncTimeout time.Duration, readEnd BackendQueueEnd) *diskQueueReader {

	d := diskQueueReader{
		readFrom:        readFrom,
		readerMetaName:  metaname,
		dataPath:        dataPath,
		maxBytesPerFile: maxBytesPerFile,
		minMsgSize:      minMsgSize,
		exitChan:        make(chan int),
		syncEvery:       syncEvery,
		ctx:             ctx,
		readBuffer:      bytes.NewBuffer(make([]byte, 0, readBufferSize)),
	}

	/*
		// init the channel to end, so if any new channel without meta will be init to read at end
		if diskEnd, ok := readEnd.(*diskQueueEndInfo); ok {
			d.confirmedQueueInfo = *diskEnd
			d.readQueueInfo = d.confirmedQueueInfo
			d.queueEndInfo = *diskEnd
			d.updateDepth()
		} else {
			nsqLog.Logf("diskqueue(%s) read end not valid %v",
				d.readFrom, readEnd)
		}
		// no need to lock here, nothing else could possibly be touching this instance
		err := d.retrieveMetaData()
		if err != nil && !os.IsNotExist(err) {
			nsqLog.LogErrorf("diskqueue(%s) failed to retrieveMetaData %v - %s",
				d.readFrom, d.readerMetaName, err)
		}
		// need update queue end with new readEnd, since it may read old from meta file
		d.UpdateQueueEnd(readEnd, false)
	*/
	return &d
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
