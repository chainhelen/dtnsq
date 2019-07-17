package nsqd

type BackendQueueEnd interface {
	Offset() int64
	TotalMsgCnt() int64
	IsSame(BackendQueueEnd) bool
}

// BackendQueue represents the behavior for the secondary message
// storage system
type BackendQueueWriter interface {
	Put(data []byte) (BackendQueueEnd, error)
	Close() error
	Delete() error
	Empty() error
	WriterFlush() (bool, bool, error)
	EndInfo()
	GetQueueReadEnd() BackendQueueEnd
	GetQueueCurWriterEnd() BackendQueueEnd
}

// ReadResult represents the result for TryReadOne()
type ReadResult struct {
	bqe  BackendQueueEnd
	Data []byte
	Err  error
}

// BackendQueueReader represents reader for current topic's consumer
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
	TryReadOne() (*ReadResult, bool)
	Confirm(start int64, end int64, endCnt int64) bool
}
