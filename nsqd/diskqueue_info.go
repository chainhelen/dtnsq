package nsqd

import "sync/atomic"

type diskQueueOffset struct {
	FileNum int64
	Pos     int64
}

type diskQueueEndInfo struct {
	EndOffset     diskQueueOffset
	virtualOffset int64
	totalMsgCnt   int64
}

func (d *diskQueueEndInfo) Offset() int64 {
	return d.virtualOffset
}

func (d *diskQueueEndInfo) TotalMsgCnt() int64 {
	return atomic.LoadInt64(&d.totalMsgCnt)
}

func (d *diskQueueEndInfo) IsSame(other BackendQueueEnd) bool {
	if otherDiskEnd, ok := other.(*diskQueueEndInfo); ok {
		return *d == *otherDiskEnd
	}
	return false
}
