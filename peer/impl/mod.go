package impl

import (
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"net"
	"sync"
	"time"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.
	var nodeAddr = conf.Socket.GetAddress()
	var newRoutable safeRoutable
	newRoutable.realTable = make(map[string]string)
	newRoutable.UpdateRoutingtable(nodeAddr, nodeAddr)
	log.Info().Msgf("%v", newRoutable)
	newNode := node{conf: conf, stopChannel: make(chan bool), routable: newRoutable}

	// Register the handler
	conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, ExecChatMessage)

	return &newNode
}

func (t *safeRoutable) UpdateRoutingtable(key string, val string) {
	t.Lock()
	defer t.Unlock()

	t.realTable[key] = val
}

func (t *safeRoutable) DeleteRoutingEntry(key string) {
	t.Lock()
	defer t.Unlock()

	delete(t.realTable, key)
}

func (t *safeRoutable) FindRoutingEntry(key string) (string, bool) {
	t.Lock()
	defer t.Unlock()
	val, ok := t.realTable[key]
	return val, ok
}

type safeRoutable struct {
	sync.Mutex
	realTable peer.RoutingTable
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// You probably want to keep the peer.Configuration on this struct:
	conf        peer.Configuration
	routable    safeRoutable
	stopChannel chan bool
}

func ExecChatMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	_, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	// When the peer receives a ChatMessage, the peer should log that message.
	log.Info().Msgf("received message = %v", msg)

	return nil
}

func (n *node) CompareHeader(pkt transport.Packet) error {
	socketAddr := n.conf.Socket.GetAddress()
	pktDest := pkt.Header.Destination
	if pktDest == socketAddr {
		// the received packet is for this node
		// -> the registry must be used to execute the callback associated with the message contained in the packet
		//log.Info().Msgf("Exec the callback")

		errProc := n.conf.MessageRegistry.ProcessPacket(pkt)
		if errProc != nil {
			//log.Error().Msgf("[CompareHeader] %v", errProc)
			return errProc
		}
		return nil
	} else {
		// the packet is to be relayed
		// -> update the RelayedBy field of the packet’s header to the peer’s socket address.
		pkt.Header.RelayedBy = socketAddr

		nextHop, ok := n.routable.FindRoutingEntry(pktDest)
		if !ok {
			return errors.New("couldn't find the peer")
		}

		err := n.conf.Socket.Send(nextHop, pkt, 0)
		//log.Info().Msgf("[CompareHeader] After sending")

		return checkTimeoutError(err, 0)
	}

	return nil
}

func checkTimeoutError(err error, timeout time.Duration) error {
	var netError net.Error
	if errors.As(err, &netError) && netError.Timeout() {
		return transport.TimeoutError(timeout)
	} else {
		return err
	}
}

// Start implements peer.Service
func (n *node) Start() error {
	// Start starts the node. It should, among other things, start listening on
	// its address using the socket.
	go func() {

		for {
			select {
			case <-n.stopChannel:
				//log.Info().Msgf("[LOG] Should end")
			default:
				//log.Info().Msgf("[LOG] Start")

				pkt, err := n.conf.Socket.Recv(1000)

				if err != nil {
					if checkTimeoutError(err, time.Duration(1000)) == transport.TimeoutError(1000) {
						continue
					}
				}

				errComp := n.CompareHeader(pkt)
				if errComp != nil {
					log.Error().Msgf("errComp = %v", errComp)
				}
			}

		}

	}()

	return nil

}

// Stop implements peer.Service
func (n *node) Stop() error {
	// Stop stops the node. This function must block until all goroutines are
	// done.
	n.stopChannel <- true
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	// Unicast sends a packet to a given destination. If the destination is the
	// same as the node's address, then the message must still be sent to the
	// node via its socket. Use transport.NewHeader to build the packet's
	// header.

	socketAddr := n.conf.Socket.GetAddress()
	header := transport.NewHeader(
		socketAddr, // source
		socketAddr, // relay
		dest,       // destination
		0,          // TTL
	)
	pktNew := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}

	nextHop, ok := n.routable.FindRoutingEntry(dest)
	if !ok {
		return errors.New("couldn't find the peer")
	}

	err := n.conf.Socket.Send(nextHop, pktNew, 0)
	return checkTimeoutError(err, 0)

}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	// AddPeer adds new known addresses to the node. It must update the
	// routing table of the node. Adding ourselves should have no effect.
	socketAddr := n.conf.Socket.GetAddress()
	for _, peerAddr := range addr {
		// lock defer unlock
		//log.Info().Msgf("[LOG] peerAddr = %v", peerAddr)
		if peerAddr != socketAddr {
			n.routable.UpdateRoutingtable(peerAddr, peerAddr)
		} else {
			log.Info().Msgf("[LOG] Add Ourselves -> Do nothing")
			return
		}
	}

}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	// GetRoutingTable returns the node's routing table. It should be a copy.

	return n.routable.realTable
	//panic("to be implemented in HW0")
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	// SetRoutingEntry sets the routing entry. Overwrites it if the entry
	// already exists. If the origin is equal to the relayAddr, then the node
	// has a new neighbor (the notion of neighboors is not needed in HW0). If
	// relayAddr is empty then the record must be deleted (and the peer has
	// potentially lost a neighbor).

	//socketAddr := n.conf.Socket.GetAddress()
	// [Question] Not quite sure about how does the routing table should be like

	if relayAddr == "" {
		//log.Info().Msgf("[SetRoutingEntry] No relay addr")
		n.routable.DeleteRoutingEntry(origin)
	} else {
		//log.Info().Msgf("[SetRoutingEntry] need relay")
		n.routable.UpdateRoutingtable(origin, relayAddr)
	}

}
