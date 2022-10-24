package impl

import (
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"math/rand"
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
	log.Info().Msgf("[NewPeer] [%v]", nodeAddr)

	var newRoutable safeRoutable
	newRoutable.Mutex = &sync.Mutex{}
	newRoutable.realTable = make(map[string]string)
	newRoutable.UpdateRoutingtable(nodeAddr, nodeAddr)

	var newStatus safeStatus
	newStatus.Mutex = &sync.Mutex{}
	newStatus.realLastStatus = make(map[string]uint)
	//newStatus.UpdateStatus(nodeAddr, 0)

	var newSentRumor safeRumorMap
	newSentRumor.Mutex = &sync.Mutex{}
	newSentRumor.realRumorMap = make(map[string][]types.Rumor)

	var newTickerAntiEn timeTicker
	if conf.AntiEntropyInterval != 0 {
		newTickerAntiEn.T = time.NewTicker(conf.AntiEntropyInterval)
		newTickerAntiEn.stopTicker = make(chan bool)
	}

	var newTickerHeartBeat timeTicker
	if conf.HeartbeatInterval != 0 {
		newTickerHeartBeat.T = time.NewTicker(conf.HeartbeatInterval)
		newTickerHeartBeat.stopTicker = make(chan bool)
	}

	var newAckChecker ackChecker
	newAckChecker.Mutex = &sync.Mutex{}
	newAckChecker.realAckChecker = make(map[string]*time.Timer)

	newNode := node{
		conf:            conf,
		stopChannel:     make(chan bool),
		tickerAntiEn:    newTickerAntiEn,
		tickerHeartBeat: newTickerHeartBeat,
		ackRecord:       newAckChecker,
		routable:        newRoutable,
		lastStatus:      newStatus,
		sentRumor:       newSentRumor}

	// Register the handler
	conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, newNode.ExecChatMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, newNode.ExecRumorsMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, newNode.ExecAckMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, newNode.ExecStatusMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, newNode.ExecEmptyMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, newNode.ExecPrivateMessage)

	return &newNode
}

type timeTicker struct {
	T          *time.Ticker
	stopTicker chan bool
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// You probably want to keep the peer.Configuration on this struct:
	conf peer.Configuration

	stopChannel     chan bool
	tickerAntiEn    timeTicker
	tickerHeartBeat timeTicker
	ackRecord       ackChecker

	routable   safeRoutable
	lastStatus safeStatus
	sentRumor  safeRumorMap
}

func (n *node) CheckHeartBeat() {
	socketAddr := n.conf.Socket.GetAddress()
	lastSeq, ok := n.lastStatus.FindStatusEntry(socketAddr)
	if !ok {
		lastSeq = 0
	}

	transMsgEmp, _ := n.conf.MessageRegistry.MarshalMessage(types.EmptyMessage{})

	newMsgRumor := types.RumorsMessage{}
	rumor := types.Rumor{
		Origin:   socketAddr,
		Sequence: lastSeq + 1,
		Msg:      &transMsgEmp,
	}

	newMsgRumor.Rumors = append(newMsgRumor.Rumors, rumor)
	header := transport.NewHeader(socketAddr, socketAddr, socketAddr, 0)

	transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newMsgRumor)
	pktRumor := transport.Packet{Header: &header, Msg: &transMsg}
	//log.Info().Msgf("[CheckHeartBeat] Call ProcessPacket")
	_ = n.conf.MessageRegistry.ProcessPacket(pktRumor)
}

func (n *node) CheckAntiEntropy() error {
	socketAddr := n.conf.Socket.GetAddress()
	neighbor := n.routable.FindNeighbor(socketAddr)

	// Put the node's status in the packet and "send it to Random Neighbor!!"

	if len(neighbor) != 0 {
		chosenNeighbor := neighbor[rand.Int()%(len(neighbor))]

		log.Info().Msgf("[CheckAntiEntropy] [%v] => Status [%v]", socketAddr, chosenNeighbor)

		header := transport.NewHeader(socketAddr, socketAddr, chosenNeighbor, 0)
		copyOfLastStatus := n.lastStatus.Freeze()
		transMsg, _ := n.conf.MessageRegistry.MarshalMessage(copyOfLastStatus)
		pktStatus := transport.Packet{Header: &header, Msg: &transMsg}

		// Send status to random neighbor
		errSend := n.conf.Socket.Send(chosenNeighbor, pktStatus, 0)
		return checkTimeoutError(errSend, 0)
	}

	log.Error().Msgf("[CheckAntiEntropy] No neighbor (Don't need to Send)")
	return nil

}

func (n *node) CompareHeader(pkt transport.Packet) error {
	socketAddr := n.conf.Socket.GetAddress()
	pktDest := pkt.Header.Destination
	if pktDest == socketAddr {
		// the received packet is for this node
		// -> the registry must be used to execute the callback associated with the message contained in the packet
		//log.Info().Msgf("[CompareHeader] Call ProcessPacket")
		_ = n.conf.MessageRegistry.ProcessPacket(pkt)

	} else {
		// else
		// the packet is to be relayed
		// -> update the RelayedBy field of the packet’s header to the peer’s socket address.
		pkt.Header.RelayedBy = socketAddr
		pkt.Header.TTL--

		nextHop, ok := n.routable.FindRoutingEntry(pktDest)
		if !ok {
			//log.Error().Msgf("[CompareHeader] couldn't find the peer of [%v]", pktDest)
			return errors.New("[CompareHeader] couldn't find the peer ")
		}

		err := n.conf.Socket.Send(nextHop, pkt, 0)
		return checkTimeoutError(err, 0)
	}

	return nil

}

func checkTimeoutError(err error, timeout time.Duration) error {
	var netError net.Error
	if errors.As(err, &netError) && netError.Timeout() {
		return transport.TimeoutError(timeout)
	}

	return err

}

func (n *node) AntiEntropyAgency() {
	go func() {
		if n.conf.AntiEntropyInterval != 0 {
			for {
				select {
				case <-n.tickerAntiEn.stopTicker:
					//Do nothing
				case <-n.tickerAntiEn.T.C:
					_ = n.CheckAntiEntropy()
				}
			}

		} // else -> do nothing
	}()
}

// Start implements peer.Service
func (n *node) Start() error {
	// Start starts the node. It should, among other things, start listening on
	// its address using the socket.
	n.AntiEntropyAgency()

	go func() {
		if n.conf.HeartbeatInterval != 0 {
			for {
				select {
				case <-n.tickerHeartBeat.stopTicker:
					//Do nothing
				case <-n.tickerHeartBeat.T.C:
					n.CheckHeartBeat()
				}
			}
		}
	}()
	go func() {

		for {
			select {
			case <-n.stopChannel:

			default:

				pkt, err := n.conf.Socket.Recv(1000)

				if err != nil {
					if errors.Is(checkTimeoutError(err, time.Duration(1000)), transport.TimeoutError(1000)) {
						continue
					}
				}
				log.Info().Msgf("[Recv] pkt and send to CompareHeader")

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

	if n.conf.AntiEntropyInterval != 0 {
		n.tickerAntiEn.T.Stop()
		n.tickerAntiEn.stopTicker <- true
	}

	if n.conf.HeartbeatInterval != 0 {
		n.tickerHeartBeat.T.Stop()
		n.tickerHeartBeat.stopTicker <- true
	}

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
		if peerAddr != socketAddr {
			n.routable.UpdateRoutingtable(peerAddr, peerAddr)
		} else {
			log.Info().Msgf("[AddPeer] Add Ourselves -> Do nothing")
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

	if relayAddr == "" {
		//log.Info().Msgf("[SetRoutingEntry] No relay addr")
		n.routable.DeleteRoutingEntry(origin)
	} else {
		//log.Info().Msgf("[SetRoutingEntry] need relay")
		n.routable.UpdateRoutingtable(origin, relayAddr)
	}

}

func (n *node) Broadcast(msg transport.Message) error {
	// Broadcast sends a packet asynchronously to all know destinations.
	// The node must not send the message to itself (to its socket),
	// but still process it.

	// 1. Create a RumorsMessage containing one Rumor (this rumor embeds the message provided in argument),
	// and send it to a random neighbour.
	socketAddr := n.conf.Socket.GetAddress()

	//check functionality
	lastSeq, _ := n.lastStatus.FindStatusEntry(socketAddr)

	// Here n.lastStatus[socketAddr] is still non-exist

	newMsgRumor := types.RumorsMessage{}
	rumor := types.Rumor{
		Origin:   socketAddr,
		Sequence: lastSeq + 1,
		Msg:      &msg,
	}

	//n.lastStatus.UpdateStatus(socketAddr, lastSeq+1)

	newMsgRumor.Rumors = append(newMsgRumor.Rumors, rumor)
	header := transport.NewHeader(socketAddr, socketAddr, socketAddr, 0)

	// Transform RumorMessages to Messages
	transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newMsgRumor)
	pktRumor := transport.Packet{Header: &header, Msg: &transMsg}
	//log.Info().Msgf("[Broadcast] Call ProcessPacket")

	// Should I add this?
	//n.sentRumor.UpdateRumorMap(socketAddr, newMsgRumor)
	//log.Info().Msgf("[BroadcastUpdateRumorMap] node(%v), SentRumor = %v", socketAddr, n.sentRumor)

	errProcess := n.conf.MessageRegistry.ProcessPacket(pktRumor)

	return checkTimeoutError(errProcess, 0)
}
