package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
	"sync"
)

func (n *node) SendAck(pkt transport.Packet) error {
	// need to change the Src and Dest in header when sending ACK
	orgSrc := pkt.Header.Source
	orgDest := pkt.Header.Destination
	log.Info().Msgf("orgSrc = %v, orgDest = %v", orgSrc, orgDest)

	// Question: functionality correct??
	var newStatus safeStatus
	newStatus.Mutex = &sync.Mutex{}
	newStatus.realLastStatus = make(map[string]uint)
	lastSeq, _ := n.lastStatus.FindStatusEntry(orgDest)
	newStatus.UpdateStatus(orgSrc, lastSeq)

	log.Info().Msgf("node ([%v]) Status = %v", n.conf.Socket.GetAddress(), n.lastStatus.realLastStatus)

	newMsgAck := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        newStatus.realLastStatus,
	}

	// Transform AckMessages to Messages
	transMsg, errMsg := n.conf.MessageRegistry.MarshalMessage(newMsgAck)
	if errMsg != nil {
		return errMsg
	}

	pkt.Header.Source = orgDest
	pkt.Header.Destination = orgSrc
	pktAct := transport.Packet{Header: pkt.Header, Msg: &transMsg}

	// Send back Act to the original sender
	errSend := n.conf.Socket.Send(pkt.Header.Destination, pktAct, 0)
	return checkTimeoutError(errSend, 0)
}

func (n *node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	log.Info().Msgf("[ExecRumorsMessage]")
	msgRumor, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	socketAddr := n.conf.Socket.GetAddress()
	log.Info().Msgf("[%v] => packet [%v]", pkt.Header.Source, socketAddr)
	isExpected := false
	neighbor := n.routable.FindRandomNeighbor(socketAddr)

	for _, rumor := range msgRumor.Rumors {
		if _, okk := n.lastStatus.GetandIncrementStatusIfEqual(pkt.Header.Destination, rumor.Sequence); okk {
			// Process this rumor's embedded message
			log.Info().Msgf("Expected Rumor")
			isExpected = true
			// find another neighbor -> Send RumorsMessage to another random neighbor

			pktRumor := transport.Packet{
				Header: pkt.Header,
				Msg:    rumor.Msg,
			}

			errProcess := n.conf.MessageRegistry.ProcessPacket(pktRumor)
			checkTimeoutError(errProcess, 0)
		}
	}

	if isExpected {
		// Transform RumorMessages to Messages
		transMsg, errMsg := n.conf.MessageRegistry.MarshalMessage(msgRumor)
		if errMsg != nil {
			return errMsg
		}

		if len(neighbor) != 0 {
			pkt.Header.Destination = neighbor[rand.Int()%(len(neighbor))]
			pkt.Header.TTL -= 1

			pktRumor := transport.Packet{Header: pkt.Header, Msg: &transMsg}
			errSend := n.conf.Socket.Send(pkt.Header.Destination, pktRumor, 0)
			log.Info().Msgf("[%v] => rumor [%v]", socketAddr, neighbor)
			checkTimeoutError(errSend, 0)
		} else {
			log.Error().Msgf("no neighbor")
		}

	}

	// send Ack to the source
	if pkt.Header.Source != socketAddr {
		log.Info().Msgf("[%v] => ack [%v]", socketAddr, pkt.Header.Source)
		errSendAck := n.SendAck(pkt)
		return errSendAck
	}

	return nil

}

func (n *node) ExecChatMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	_, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// When the peer receives a ChatMessage, the peer should log that message.
	log.Info().Msgf("[ExecChatMessage] received ChatMessage = %v", msg)

	return nil
}

func (n *node) ExecAckMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	msgAck, ok := msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	//n.conf.MessageRegistry.UnmarshalMessage(pkt.Msg, msgAck)
	// When the peer receives a ChatMessage, the peer should log that message.
	log.Info().Msgf("[ExecAckMessage] received AckMessage = %v", msg)
	log.Info().Msgf("[ExecAckMessage] Ack Message = %v", msgAck.Status)

	return nil
}
