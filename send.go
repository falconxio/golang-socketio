package gosocketio

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/graarh/golang-socketio/protocol"
)

var (
	ErrorSendTimeout     = errors.New("Timeout")
	ErrorSocketOverflood = errors.New("Socket overflood")
	ErrorSocketClosed    = errors.New("Socket closed")
)

/**
Send message packet to socket
*/
func send(msg *protocol.Message, c *Channel, args interface{}) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("socket.io send panic: %v", recovered)
		}
	}()

	if args != nil {
		json, err := json.Marshal(&args)
		if err != nil {
			return err
		}

		msg.Args = string(json)
	}

	command, err := protocol.Encode(msg)
	if err != nil {
		return err
	}

	if !c.IsAlive() {
		return ErrorSocketClosed
	}

	select {
	case c.out <- command:
		return nil
	case <-c.done:
		return ErrorSocketClosed
	default:
		return ErrorSocketOverflood
	}
}

/**
Create packet based on given data and send it
*/
func (c *Channel) Emit(method string, namespace string, args interface{}) error {
	msg := &protocol.Message{
		Type:      protocol.MessageTypeEmit,
		Method:    method,
		Namespace: namespace,
	}

	return send(msg, c, args)
}

/**
Create ack packet based on given data and send it and receive response
*/
func (c *Channel) Ack(method string, args interface{}, timeout time.Duration) (string, error) {
	msg := &protocol.Message{
		Type:   protocol.MessageTypeAckRequest,
		AckId:  c.ack.getNextId(),
		Method: method,
	}

	waiter := make(chan string, 1)
	c.ack.addWaiter(msg.AckId, waiter)

	if err := send(msg, c, args); err != nil {
		c.ack.removeWaiter(msg.AckId)
		return "", err
	}

	select {
	case result := <-waiter:
		return result, nil
	case <-time.After(timeout):
		c.ack.removeWaiter(msg.AckId)
		return "", ErrorSendTimeout
	}
}
