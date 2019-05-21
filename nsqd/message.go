package nsqd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

type MessageID [MsgIDLength]byte

const (
	UNKONW_STATUS = MessageDtType(0)
	PRE_STATUS    = MessageDtType(1)
	CANCEL_STATUS = MessageDtType(2)
	COMMIT_STATUS = MessageDtType(3)
)

type MessageDtType int

type Message struct {
	ID        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16

	// for in-flight handling
	deliveryTS time.Time
	clientID   int64
	pri        int64
	index      int
	deferred   time.Duration

	//for backend queue
	Offset        int64
	queueCntIndex int64

	// for dt
	isDt       bool
	dtStatus   MessageDtType
	dtPreMsgId MessageID
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

// TODO mutex?
func (m *Message) SetDtStatus(dtStatus MessageDtType) {
	m.isDt = true
	m.dtStatus = dtStatus
}

// TODO mutex?
func (m *Message) GetDtStatus() (bool, MessageDtType) {
	return m.isDt, m.dtStatus
}

func (m *Message) ExtractPreMsgIdFromCmtMsgBody(body []byte) {
	var h MessageID
	copy(h[:], body[0:MsgIDLength])
	m.dtPreMsgId = h
}

func (m *Message) GetDtPreMsgId() MessageID {
	return m.dtPreMsgId
}

func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// decodeMessage deserializes data (as []byte) and creates a new Message
// message format:
// [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
// |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
// |       8-byte         ||    ||                 16-byte                      || N-byte
// ------------------------------------------------------------------------------------------...
//   nanosecond timestamp    ^^                   message ID                       message body
//                        (uint16)
//                         2-byte
//                        attempts
func decodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	copy(msg.ID[:], b[10:10+MsgIDLength])
	msg.Body = b[10+MsgIDLength:]

	return &msg, nil
}

func writeMessageToBackend(buf *bytes.Buffer, msg *Message, put func(x []byte) (int64, int64, error)) error {
	buf.Reset()
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	_, _, err = put(buf.Bytes())
	return err
}
