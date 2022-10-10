package udp

import (
	"errors"
	"github.com/rs/zerolog/log"
	"math"
	"net"
	"sync"
	"time"

	"go.dedis.ch/cs438/transport"
)

const bufSize = 65000

var counter uint32 // initialized by default to 0

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{
		incomings: transport.Packet{},
		//traffic:   traffic.NewTraffic(),
	}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
	sync.RWMutex
	incomings transport.Packet
	//traffic   *traffic.Traffic
}

type packets struct {
	sync.Mutex
	data []transport.Packet
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	*UDP
	conn       *net.UDPConn
	socketAddr string

	ins  packets
	outs packets
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {

	serverAddr, errRes := net.ResolveUDPAddr("udp", address)

	if errRes != nil {
		log.Error().Msgf("[Create] %v", errRes)
		return &Socket{}, errRes
	}

	listenConn, errLis := net.ListenUDP("udp", serverAddr)
	if errLis != nil {
		log.Error().Msgf("%v", errLis)
		return &Socket{}, errLis
	}

	return &Socket{
		UDP:  n,
		conn: listenConn,

		socketAddr: listenConn.LocalAddr().String(),

		ins:  packets{},
		outs: packets{},
	}, nil

	//panic("Create Socket")

}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	err := s.conn.Close()
	if err != nil {
		return err
	}
	return nil
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	// Send sends a msg to the destination. If the timeout is reached without
	// having sent the message, returns a TimeoutError. A value of 0 means no
	// timeout.
	//log.Info().Msgf("[Send]")

	buf, errMar := pkt.Marshal()

	if errMar != nil {
		log.Error().Msgf("[Marshal error] %v", errMar)
		return errMar
	}

	destAddr, _ := net.ResolveUDPAddr("udp", dest)
	connSend, errDial := net.DialUDP("udp", nil, destAddr)

	if errDial != nil {
		log.Error().Msgf("[Dial error] %v", errDial)
		return errDial
	}

	if timeout == 0 {
		timeout = math.MaxInt64
	}
	connSend.SetWriteDeadline(time.Now().Add(timeout))
	_, errWrite := connSend.Write(buf)

	if errWrite != nil {
		log.Error().Msgf("[Write error] %v", errWrite)
		return checkTimeoutError(errWrite, timeout)
	}

	s.outs.add(pkt)
	//log.Info().Msgf("[Send] Successful")
	return nil

}

func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	// Recv implements transport.Socket. It blocks until a packet is received, or
	// the timeout is reached. In the case the timeout is reached, return a
	// TimeoutErr.

	//log.Info().Msgf("[Recv]")

	buffer := make([]byte, bufSize)

	if timeout == 0 {
		timeout = math.MaxInt64
	}

	s.conn.SetReadDeadline(time.Now().Add(timeout))
	n, _, errRead := s.conn.ReadFromUDP(buffer)

	if errRead != nil {
		//log.Error().Msgf("[Read error]")
		err := checkTimeoutError(errRead, timeout)
		return transport.Packet{}, err
	}

	s.UDP.Lock()
	defer s.UDP.Unlock()
	errUnmar := s.UDP.incomings.Unmarshal(buffer[:n])

	if errUnmar != nil {
		log.Error().Msgf("[Unmarshal error]")
		return transport.Packet{}, errUnmar
	}

	s.ins.add(s.UDP.incomings)
	return s.UDP.incomings.Copy(), nil
}

func (s *Socket) GetAddress() string {
	// GetAddress implements transport.Socket. It returns the address assigned. Can
	// be useful in the case one provided a :0 address, which makes the system use a
	// random free port.

	//s.socketAddr = s.conn.LocalAddr().String()

	return s.conn.LocalAddr().String()

}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	// GetIns must return all the messages received so far.

	return s.ins.getAll()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	// GetOuts must return all the messages sent so far.

	return s.outs.getAll()
}

func checkTimeoutError(err error, timeout time.Duration) error {
	var netError net.Error
	if errors.As(err, &netError) && netError.Timeout() {
		return transport.TimeoutError(timeout)
	} else {
		return err
	}
}

func (p *packets) add(pkt transport.Packet) {
	p.Lock()
	defer p.Unlock()

	p.data = append(p.data, pkt.Copy())
}

func (p *packets) getAll() []transport.Packet {
	p.Lock()
	defer p.Unlock()

	//log.Info().Msgf("[getAll] Packet len = %v", len(p.data))
	res := make([]transport.Packet, len(p.data))

	for i, pkt := range p.data {
		res[i] = pkt.Copy()
	}

	return res
}
