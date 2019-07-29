package nsqd

import (
	"errors"
	"github.com/chainhelen/dtnsq/internal/test"
	"github.com/chainhelen/go-dtnsq"
	"io"
	"io/ioutil"
	"net"
	"testing"
	"time"
)

func mustStartNSQD(opts *Options) (*net.TCPAddr, *net.TCPAddr, *NSQD) {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	opts.HTTPSAddress = "127.0.0.1:0"
	if opts.DataPath == "" {
		tmpDir, err := ioutil.TempDir("", "nsq-test-")
		if err != nil {
			panic(err)
		}
		opts.DataPath = tmpDir
	}
	nsqd, err := New(opts)
	if err != nil {
		panic(err)
	}
	go func() {
		err := nsqd.Main()
		if err != nil {
			panic(err)
		}
	}()
	return nsqd.RealTCPAddr(), nsqd.RealHTTPAddr(), nsqd
}

func mustConnectNSQD(tcpAddr *net.TCPAddr) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), time.Second)
	if err != nil {
		return nil, err
	}
	conn.Write(nsq.MagicV2)
	return conn, nil
}

func identify(t *testing.T, conn io.ReadWriter, extra map[string]interface{}, f int32) []byte {
	ci := make(map[string]interface{})
	ci["client_id"] = "test"
	ci["feature_negotiation"] = true
	if extra != nil {
		for k, v := range extra {
			ci[k] = v
		}
	}
	cmd, _ := nsq.Identify(ci)
	_, err := cmd.WriteTo(conn)
	test.Nil(t, err)
	resp, err := nsq.ReadResponse(conn)
	test.Nil(t, err)
	frameType, data, err := nsq.UnpackResponse(resp)
	test.Nil(t, err)
	test.Equal(t, frameType, f)
	return data
}

func sub(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	_, err := nsq.Subscribe(topicName, channelName).WriteTo(conn)
	test.Nil(t, err)
	readValidate(t, conn, frameTypeResponse, "OK")
}

func authCmd(t *testing.T, conn io.ReadWriter, authSecret string, expectSuccess string) {
	auth, _ := nsq.Auth(authSecret)
	_, err := auth.WriteTo(conn)
	test.Nil(t, err)
	if expectSuccess != "" {
		readValidate(t, conn, nsq.FrameTypeResponse, expectSuccess)
	}
}

func subFail(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	_, err := nsq.Subscribe(topicName, channelName).WriteTo(conn)
	test.Nil(t, err)
	resp, err := nsq.ReadResponse(conn)
	frameType, _, err := nsq.UnpackResponse(resp)
	test.Equal(t, frameTypeError, frameType)
}

func readValidate(t *testing.T, conn io.Reader, f int32, d string) []byte {
	resp, err := nsq.ReadResponse(conn)
	test.Nil(t, err)
	frameType, data, err := nsq.UnpackResponse(resp)
	test.Nil(t, err)
	test.Equal(t, f, frameType)
	test.Equal(t, d, string(data))
	return data
}

type errorBackendQueue struct{}

func (d *errorBackendQueue) Put([]byte) (BackendQueueEnd, error) {
	return nil, errors.New("never gonna happen")
}
func (d *errorBackendQueue) ReadChan() chan []byte                             { return nil }
func (d *errorBackendQueue) Close() error                                      { return nil }
func (d *errorBackendQueue) Delete() error                                     { return nil }
func (d *errorBackendQueue) Depth() int64                                      { return 0 }
func (d *errorBackendQueue) Empty() error                                      { return nil }
func (d *errorBackendQueue) ReaderFlush() error                                { return nil }
func (d *errorBackendQueue) GetQueueReadEnd() BackendQueueEnd                  { return nil }
func (d *errorBackendQueue) GetQueueCurMemRead() BackendQueueEnd               { return nil }
func (d *errorBackendQueue) UpdateBackendQueueEnd(BackendQueueEnd)             {}
func (d *errorBackendQueue) TryReadOne() (*ReadResult, bool)                   { return nil, false }
func (d *errorBackendQueue) Confirm(start int64, end int64, endCnt int64) bool { return true }

type errorRecoveredBackendQueue struct{ errorBackendQueue }
