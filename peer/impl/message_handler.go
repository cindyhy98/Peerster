package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
	"sync"
)

// Contains TODO: Should it be locked????
func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (n *node) SendAck(pkt transport.Packet) error {
	// need to change the Src and Dest in header when sending ACK
	orgSrc := pkt.Header.Source
	orgDest := pkt.Header.Destination
	log.Info().Msgf("orgSrc = [%v], orgDest = [%v]", orgSrc, orgDest)

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
	header := transport.NewHeader(orgDest, orgDest, orgSrc, 0)

	// Transform AckMessages to Messages
	transMsg, errMsg := n.conf.MessageRegistry.MarshalMessage(newMsgAck)
	if errMsg != nil {
		return errMsg
	}

	pktAct := transport.Packet{Header: &header, Msg: &transMsg}

	// Send back Act to the original sender
	errSend := n.conf.Socket.Send(header.Destination, pktAct, 0)
	return checkTimeoutError(errSend, 0)
}

func (n *node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	msgRumor, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	socketAddr := n.conf.Socket.GetAddress()
	log.Info().Msgf("[ExecRumorsMessage] [%v] => packet [%v]", pkt.Header.Source, socketAddr)
	isExpected := false
	neighbor := n.routable.FindNeighbor(socketAddr)

	for _, rumor := range msgRumor.Rumors {
		if _, okk := n.lastStatus.GetandIncrementStatusIfEqual(pkt.Header.Destination, rumor.Sequence); okk {

			// TODO: check if the orgSrc is already our neighbor, if not -> add to the routing table
			if !Contains(neighbor, pkt.Header.Source) {
				log.Info().Msgf("[ExecRumorsMessage] Add [%v]:[%v] to routing table", pkt.Header.Source, pkt.Header.RelayedBy)

				// TODO: Should we check if the entry exists before updating the routing table? Or doesn't matter
				n.routable.UpdateRoutingtable(pkt.Header.Source, pkt.Header.RelayedBy)
			}

			log.Info().Msgf("[ExecRumorsMessage] Expected Rumor")
			isExpected = true

			pktRumor := transport.Packet{
				Header: pkt.Header,
				Msg:    rumor.Msg,
			}

			errProcess := n.conf.MessageRegistry.ProcessPacket(pktRumor)
			err := checkTimeoutError(errProcess, 0)
			if err != nil {
				return err
			}
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
			pkt.Header.RelayedBy = socketAddr // TODO: Should I change this?
			pkt.Header.TTL -= 1

			pktRumor := transport.Packet{Header: pkt.Header, Msg: &transMsg}
			errSend := n.conf.Socket.Send(pkt.Header.Destination, pktRumor, 0)
			log.Info().Msgf("[ExecRumorsMessage] [%v] => rumor [%v]", socketAddr, neighbor)
			err := checkTimeoutError(errSend, 0)
			if err != nil {
				return err
			}
		} else {
			log.Error().Msgf("[ExecRumorsMessage] No neighbor (Don't need to Send)")
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
	msgChat, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// When the peer receives a ChatMessage, the peer should log that message.
	log.Info().Msgf("[ExecChatMessage] received ChatMessage = %v", msgChat.Message)

	return nil
}

func (n *node) ExecAckMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	msgAck, ok := msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msgf("[ExecAckMessage] AckMessage status = %v", msgAck.Status)

	return nil
}

func (n *node) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	msgStatus, ok := msg.(*types.StatusMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msgf("[ExecAckMessage] AckMessage status = %v", msgStatus)

	return nil
}
