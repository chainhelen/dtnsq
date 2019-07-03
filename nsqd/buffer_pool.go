package nsqd

import (
	"bytes"
	"sync"
)

var bp sync.Pool

func init() {
	bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

func bufferPoolGet() *bytes.Buffer {
	bps := bp.Get().(*bytes.Buffer)
	bps.Reset()
	return bps
}

func bufferPoolPut(b *bytes.Buffer) {
	bp.Put(b)
}
