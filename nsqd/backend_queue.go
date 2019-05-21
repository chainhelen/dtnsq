package nsqd

type BackendQueueOffset interface {
	Offset() int64
}

type BackendQueueEnd interface {
	Offset() int64
	TotalMsgCnt() int64
	IsSame(BackendQueueEnd) bool
}

// BackendQueue represents the behavior for the secondary message
// storage system
type BackendQueueWriter interface {
	Put(data []byte) (int64, int64, error)
	//ReadChan() chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	//	Depth() int64
	Empty() error
	Flush() error
	EndInfo()
	GetQueueReadStart() BackendQueueEnd
}

type ReadResult struct {
	Offset    int64
	MovedSize int64
	CurCnt    int64
	Data      []byte
	Err       error
}

// for topic producer
type BackendQueueReader interface {
	Put([]byte) (int64, int64, error)
	Close() error
	Delete() error
	Empty() error
	Depth() int64
	//Flush(bool) error
	//GetQueueWriteEnd() BackendQueueEnd
	//GetQueueReadStart() BackendQueueEnd
	//GetQueueReadEnd() BackendQueueEnd
	//RollbackWrite(BackendOffset, uint64) error
	//ResetWriteEnd(BackendOffset, int64) error
}
