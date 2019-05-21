package nsqd

type dummyBackendQueue struct {
	readChan chan []byte
}

func newDummyBackendQueueWriter() BackendQueueWriter {
	return &dummyBackendQueue{readChan: make(chan []byte)}
}

func newDummyBackendQueueReader() BackendQueueReader {
	return &dummyBackendQueue{readChan: make(chan []byte)}
}

func (d *dummyBackendQueue) Put([]byte) (int64, int64, error) {
	return 0, 0, nil
}

func (d *dummyBackendQueue) ReadChan() chan []byte {
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

func (d *dummyBackendQueue) Flush() error {
	return nil
}

func (d *dummyBackendQueue) GetQueueReadStart() BackendQueueEnd {
	e := &diskQueueEndInfo{}
	return e
}
