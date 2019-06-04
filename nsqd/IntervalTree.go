package nsqd

import (
	"github.com/Workiva/go-datastructures/augmentedtree"
)

type QueueInterval struct {
	start  int64
	end    int64
	endCnt int64
}

func (QI *QueueInterval) Start() int64 {
	return QI.start
}
func (QI *QueueInterval) End() int64 {
	return QI.end
}
func (QI *QueueInterval) EndCnt() int64 {
	return QI.endCnt
}

// the augmentedtree use the low and the id to determin if the interval is the duplicate
// so here we use the end as the id of segment
func (QI *QueueInterval) ID() uint64 {
	return uint64(QI.end)
}

func (QI *QueueInterval) LowAtDimension(dim uint64) int64 {
	return QI.start
}

func (QI *QueueInterval) HighAtDimension(dim uint64) int64 {
	return QI.end
}

func (QI *QueueInterval) OverlapsAtDimension(inter augmentedtree.Interval, dim uint64) bool {
	if inter.HighAtDimension(dim) < QI.start {
		return false
	}
	if inter.LowAtDimension(dim) > QI.end {
		return false
	}
	return true
}

type IntervalTree struct {
	tr augmentedtree.Tree
}

func NewIntervalTree() *IntervalTree {
	return &IntervalTree{
		tr: augmentedtree.New(1),
	}
}

func (IT *IntervalTree) AddOrMerge(inter *QueueInterval) *QueueInterval {
	overlaps := IT.tr.Query(inter)
	if len(overlaps) == 1 && overlaps[0].LowAtDimension(0) <= inter.LowAtDimension(0) &&
		overlaps[0].HighAtDimension(0) >= inter.HighAtDimension(0) {
		return overlaps[0].(*QueueInterval)
	} else if 0 == len(overlaps) {
		IT.tr.Add(inter)
		return inter
	} else {
		qi := &QueueInterval{}
		qi.start = inter.Start()
		qi.end = inter.End()
		qi.endCnt = inter.EndCnt()

		for _, v := range overlaps {
			if v.LowAtDimension(0) < qi.start {
				qi.start = v.LowAtDimension(0)
			}
			if v.LowAtDimension(0) > qi.end {
				qi.end = v.HighAtDimension(0)
				qi.endCnt = v.(*QueueInterval).EndCnt()
			}
			IT.tr.Delete(v)
		}
		return qi
	}
	return nil
}

func (IT *IntervalTree) Len() int64 {
	return int64(IT.tr.Len())
}

func (self *IntervalTree) DeleteInterval(inter *QueueInterval) {
	overlaps := self.tr.Query(inter)
	for _, v := range overlaps {
		if v.LowAtDimension(1) == inter.LowAtDimension(1) &&
			v.HighAtDimension(1) == inter.HighAtDimension(1) {
			self.tr.Delete(v)
		}
	}
}
