package nsqd

type dummyBackendQueue struct {
	readChan chan ReadResult
}

func newDummyBackendQueueWriter() BackendQueueWriter {
	return &dummyBackendQueue{readChan: make(chan ReadResult)}
}

func newDummyBackendQueueReader() BackendQueueReader {
	return &dummyBackendQueue{readChan: make(chan ReadResult)}
}

func (d *dummyBackendQueue) GetQueueCurMemRead() BackendQueueEnd {
	return nil
}

func (d *dummyBackendQueue) Put([]byte) (BackendQueueEnd, error) {
	return nil, nil
}

func (d *dummyBackendQueue) ReadChan() chan ReadResult {
	return d.readChan
}

func (d *dummyBackendQueue) Close() error {
	return nil
}

func (d *dummyBackendQueue) Delete() error {
	return nil
}

func (d *dummyBackendQueue) Depth() int64 {
	return int64(0)
}

func (d *dummyBackendQueue) Empty() error {
	return nil
}

func (d *dummyBackendQueue) EndInfo() {

}

func (d *dummyBackendQueue) WriterFlush() (bool, bool, error) {
	return false, false, nil
}

func (d *dummyBackendQueue) ReaderFlush() error {
	return nil
}

func (d *dummyBackendQueue) GetQueueReadEnd() BackendQueueEnd {
	e := &diskQueueEndInfo{}
	return e
}

func (d *dummyBackendQueue) GetQueueCurWriterEnd() BackendQueueEnd {
	e := &diskQueueEndInfo{}
	return e
}

func (d *dummyBackendQueue) TryReadOne() (*ReadResult, bool) {
	return nil, false

}

func (d *dummyBackendQueue) UpdateBackendQueueEnd(BackendQueueEnd) {

}

func (d *dummyBackendQueue) Confirm(start int64, end int64, endCnt int64) bool {
	return false
}
