package nsqd

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/chainhelen/dtnsq/internal/test"
)

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()
	conn, _ := mustConnectNSQD(tcpAddr)
	defer conn.Close()

	topicName := "test_put_message" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch")
	client := newClientV2(0, conn, &context{nsqd})
	client.SetReadyCount(25)
	err := channel1.AddClient(client.ID, client)
	test.Equal(t, err, nil)

	var id MessageID
	msg := NewMessage(id, []byte("test"))

	// put message without triggering channel to consume msg
	topic.PutMessage(msg)
	topic.backend.WriterFlush()
	channel1.backend.UpdateBackendQueueEnd(topic.backend.GetQueueReadEnd())

	res, _ := channel1.tryReadOne()
	test.NotNil(t, res)
	rmsg, _ := decodeMessage(res.Data)
	test.NotNil(t, rmsg)

	test.Equal(t, msg.ID, rmsg.ID)
	test.Equal(t, msg.Body, rmsg.Body)
}

// ensure that both channels get the same message
func TestPutMessage2Chan(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()
	conn, _ := mustConnectNSQD(tcpAddr)
	defer conn.Close()

	topicName := "test_put_message_2chan" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch1")
	channel2 := topic.GetChannel("ch2")

	client := newClientV2(0, conn, &context{nsqd})
	client.SetReadyCount(25)
	err := channel1.AddClient(client.ID, client)
	test.Equal(t, err, nil)
	err = channel2.AddClient(client.ID, client)
	test.Equal(t, err, nil)

	var id MessageID
	msg := NewMessage(id, []byte("test"))

	// put message without triggering channel to consume msg
	topic.PutMessage(msg)
	topic.backend.WriterFlush()
	channel1.backend.UpdateBackendQueueEnd(topic.backend.GetQueueReadEnd())
	channel2.backend.UpdateBackendQueueEnd(topic.backend.GetQueueReadEnd())

	res1, _ := channel1.tryReadOne()
	test.NotNil(t, res1)
	rmsg1, _ := decodeMessage(res1.Data)
	test.NotNil(t, rmsg1)
	test.Equal(t, msg.ID, rmsg1.ID)
	test.Equal(t, msg.Body, rmsg1.Body)

	res2, _ := channel2.tryReadOne()
	test.NotNil(t, res2)
	rmsg2, _ := decodeMessage(res2.Data)
	test.NotNil(t, rmsg2)
	test.Equal(t, msg.ID, rmsg2.ID)
	test.Equal(t, msg.Body, rmsg2.Body)
}

func TestInFlightWorker(t *testing.T) {
	count := 250

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MsgTimeout = 100 * time.Millisecond
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_in_flight_worker" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	for i := 0; i < count; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
	}

	channel.Lock()
	inFlightMsgs := len(channel.inFlightMessages)
	channel.Unlock()
	test.Equal(t, count, inFlightMsgs)

	channel.inFlightMutex.Lock()
	inFlightPQMsgs := len(channel.inFlightPQ)
	channel.inFlightMutex.Unlock()
	test.Equal(t, count, inFlightPQMsgs)

	// the in flight worker has a resolution of 100ms so we need to wait
	// at least that much longer than our msgTimeout (in worst case)
	time.Sleep(4 * opts.MsgTimeout)

	channel.Lock()
	inFlightMsgs = len(channel.inFlightMessages)
	channel.Unlock()
	test.Equal(t, count, inFlightMsgs)

	channel.inFlightMutex.Lock()
	inFlightPQMsgs = len(channel.inFlightPQ)
	channel.inFlightMutex.Unlock()
	test.Equal(t, count, inFlightPQMsgs)

}

func TestChannelEmpty(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 25)
	for i := 0; i < 25; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
		msgs = append(msgs, msg)
	}

	channel.RequeueMessage(0, msgs[len(msgs)-1].ID, 100*time.Millisecond)
	test.Equal(t, 24, len(channel.inFlightMessages))
	test.Equal(t, 24, len(channel.inFlightPQ))
	test.Equal(t, 1, len(channel.deferredMessages))
	test.Equal(t, 1, len(channel.deferredPQ))

	channel.Empty()

	test.Equal(t, 0, len(channel.inFlightMessages))
	test.Equal(t, 0, len(channel.inFlightPQ))
	test.Equal(t, 0, len(channel.deferredMessages))
	test.Equal(t, 0, len(channel.deferredPQ))
	test.Equal(t, int64(0), channel.Depth())
}

func TestChannelEmptyConsumer(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, _ := mustConnectNSQD(tcpAddr)
	defer conn.Close()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")
	client := newClientV2(0, conn, &context{nsqd})
	client.SetReadyCount(25)
	err := channel.AddClient(client.ID, client)
	test.Equal(t, err, nil)

	for i := 0; i < 25; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
		client.SendingMessage()
	}

	for _, cl := range channel.clients {
		stats := cl.Stats()
		test.Equal(t, int64(25), stats.InFlightCount)
	}

	channel.Empty()

	for _, cl := range channel.clients {
		stats := cl.Stats()
		test.Equal(t, int64(0), stats.InFlightCount)
	}
}

func TestMaxChannelConsumers(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MaxChannelConsumers = 1
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, _ := mustConnectNSQD(tcpAddr)
	defer conn.Close()

	topicName := "test_max_channel_consumers" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	client1 := newClientV2(1, conn, &context{nsqd})
	client1.SetReadyCount(25)
	err := channel.AddClient(client1.ID, client1)
	test.Equal(t, err, nil)

	client2 := newClientV2(2, conn, &context{nsqd})
	client2.SetReadyCount(25)
	err = channel.AddClient(client2.ID, client2)
	test.NotEqual(t, err, nil)
}

func TestChannelHealth(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)

	tcpAddr, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, _ := mustConnectNSQD(tcpAddr)
	defer conn.Close()

	topic := nsqd.GetTopic("test")
	channel := topic.GetChannel("channel")
	client := newClientV2(0, conn, &context{nsqd})
	client.SetReadyCount(25)
	err := channel.AddClient(client.ID, client)
	test.Equal(t, err, nil)

	sendOverChan := make(chan bool)
	wg := &sync.WaitGroup{}

	go func() {
		wg.Add(1)
		clientMsgChan := channel.clientMsgChan
		count := 0
		for {
			select {
			case <-clientMsgChan:
				count++
				if count == 2 {
					clientMsgChan = nil
				}
			case <-sendOverChan:
				wg.Done()
				return
			}
		}
	}()
	time.Sleep(time.Duration(50) * time.Millisecond)

	msg := NewMessage(topic.GenerateID(), make([]byte, 100))
	err = channel.put(msg)
	test.Nil(t, err)

	url := fmt.Sprintf("http://%s/ping", httpAddr)
	resp, err := http.Get(url)

	resp, err = http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "OK", string(body))

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = channel.put(msg)
	test.Nil(t, err)

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = channel.put(msg)
	test.NotNil(t, err)

	resp, err = http.Get(url)
	test.Nil(t, err)

	test.Equal(t, 500, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "NOK - (test:channel) put client failed, len(clients) == 1", string(body))

	close(sendOverChan)
	wg.Wait()
}
