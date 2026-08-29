package gosocketio

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/graarh/golang-socketio/protocol"
	"github.com/graarh/golang-socketio/transport"
)

const (
	queueBufferSize = 500
)

var (
	ErrorWrongHeader = errors.New("Wrong header")
)

/**
engine.io header to send or receive
*/
type Header struct {
	Sid          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int      `json:"pingInterval"`
	PingTimeout  int      `json:"pingTimeout"`
}

/**
socket.io connection handler

use IsAlive to check that handler is still working
use Dial to connect to websocket
use In and Out channels for message exchange
Close message means channel is closed
ping is automatic
*/
type Channel struct {
	conn transport.Connection

	out    chan string
	header Header

	alive     bool
	aliveLock sync.Mutex
	done      chan struct{}

	ack ackProcessor

	server        *Server
	ip            string
	requestHeader http.Header
}

/**
create channel, map, and set active
*/
func (c *Channel) initChannel() {
	//TODO: queueBufferSize from constant to server or client variable
	c.out = make(chan string, queueBufferSize)
	c.ack.resultWaiters = make(map[int](chan string))
		c.alive = true
	c.done = make(chan struct{})

}

/**
Get id of current socket connection
*/
func (c *Channel) Id() string {
	return c.header.Sid
}

/**
Checks that Channel is still alive
*/
func (c *Channel) IsAlive() bool {
	c.aliveLock.Lock()
	defer c.aliveLock.Unlock()

	return c.alive
}

/**
Close channel
*/
func closeChannel(c *Channel, m *methods, reasons ...error) error {
	c.aliveLock.Lock()
	if !c.alive {
		c.aliveLock.Unlock()
		return nil
	}
	c.alive = false
	close(c.done)
	c.aliveLock.Unlock()

	c.conn.Close()

	// Drain queued messages before notifying the output loop to stop.
	for len(c.out) > 0 {
		<-c.out
	}
	c.out <- protocol.CloseMessage

	// Invoke callbacks after releasing aliveLock so callbacks may inspect or close the channel.
	m.callLoopEvent(c, OnDisconnection, reasons...)

	overfloodedLock.Lock()
	delete(overflooded, c)
	overfloodedLock.Unlock()

	return nil
}

//incoming messages loop, puts incoming messages to In channel
func inLoop(c *Channel, m *methods) error {
	for {
		pkg, err := c.conn.GetMessage()
		if err != nil {
			return closeChannel(c, m, err)
		}
		msg, err := protocol.Decode(pkg)
		if err != nil {
			detailedError := fmt.Errorf("error decoding message: %s", pkg)
			closeChannel(c, m, protocol.ErrorWrongPacket, err, detailedError)
			return err
		}

		switch msg.Type {
			case protocol.MessageTypeOpen:
				if len(msg.Source) < 2 {
					return closeChannel(c, m, ErrorWrongHeader)
				}
				if err := json.Unmarshal([]byte(msg.Source[1:]), &c.header); err != nil {
					return closeChannel(c, m, ErrorWrongHeader, err)
				}
				m.callLoopEvent(c, OnConnection)
			case protocol.MessageTypePing:
				select {
				case c.out <- protocol.PongMessage:
				case <-c.done:
					return nil
				}
		case protocol.MessageTypePong:
			default:
				m.processIncomingMessage(c, msg)
		}
	}
	return nil
}

var overflooded map[*Channel]struct{} = make(map[*Channel]struct{})
var overfloodedLock sync.Mutex

func AmountOfOverflooded() int64 {
	overfloodedLock.Lock()
	defer overfloodedLock.Unlock()

	return int64(len(overflooded))
}

/**
outgoing messages loop, sends messages from channel to socket
*/
func outLoop(c *Channel, m *methods) error {
	for {
		outBufferLen := len(c.out)
		if outBufferLen >= queueBufferSize-1 {
			return closeChannel(c, m, ErrorSocketOverflood)
		} else if outBufferLen > int(queueBufferSize/2) {
			overfloodedLock.Lock()
			overflooded[c] = struct{}{}
			overfloodedLock.Unlock()
		} else {
			overfloodedLock.Lock()
			delete(overflooded, c)
			overfloodedLock.Unlock()
		}

		msg := <-c.out
		if msg == protocol.CloseMessage {
			return nil
		}

		err := c.conn.WriteMessage(msg)
		if err != nil {
			return closeChannel(c, m, err)
		}
	}
	return nil
}

/**
Pinger sends ping messages for keeping connection alive
*/
func pinger(c *Channel) {
	for {
		interval, _ := c.conn.PingParams()
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-c.done:
			timer.Stop()
			return
		}

		select {
		case c.out <- protocol.PingMessage:
		case <-c.done:
			return
		}
	}
}
