package nsqd

//type BackendQueueOffset interface {
//	Offset() int64
//}

type BackendQueueEnd interface {
	Offset() int64
	TotalMsgCnt() int64
	IsSame(BackendQueueEnd) bool
}

// BackendQueue represents the behavior for the secondary message
// storage system
type BackendQueueWriter interface {
	Put(data []byte) (BackendQueueEnd, error)
	//ReadChan() chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	//	Depth() int64
	Empty() error
	WriterFlush() (bool, bool, error)
	EndInfo()
	GetQueueReadEnd() BackendQueueEnd
	GetQueueCurWriterEnd() BackendQueueEnd
	// GetUpdatedBackendQueueEndChan() <-chan BackendQueueEnd
}

type ReadResult struct {
	//Offset    int64
	///MovedSize int64
	//CurCnt    int64
	bqe  BackendQueueEnd
	Data []byte
	Err  error
}

// for topic producer
type BackendQueueReader interface {
	Put([]byte) (BackendQueueEnd, error)
	Close() error
	Delete() error
	Empty() error
	Depth() int64
	ReaderFlush() error
	GetQueueReadEnd() BackendQueueEnd
	GetQueueCurMemRead() BackendQueueEnd
	UpdateBackendQueueEnd(BackendQueueEnd)
	//TryReadOne() (*ReadResult, bool)
	// ReadChan() chan ReadResult
	TryReadOne() (*ReadResult, bool)
	Confirm(start int64, end int64, endCnt int64) bool
}
