package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
	"sort"
	"time"
)

func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (n *node) SendMissingRumor(Dest string, rumor []types.Rumor) error {
	socketAddr := n.conf.Socket.GetAddress()
	log.Info().Msgf("[SendMissingRumor] [%v] => Rumor [%v]", socketAddr, Dest)

	header := transport.NewHeader(socketAddr, socketAddr, Dest, 0)
	transMsg, _ := n.conf.MessageRegistry.MarshalMessage(types.RumorsMessage{Rumors: rumor})

	pktRumor := transport.Packet{Header: &header, Msg: &transMsg}
	errSend := n.conf.Socket.Send(Dest, pktRumor, 0)
	return checkTimeoutError(errSend, 0)
}

func (n *node) SendStatusBackToRemote(remoteAddr string) error {
	socketAddr := n.conf.Socket.GetAddress()
	log.Info().Msgf("[SendStatusBackToRemote] [%v] => Status [%v]", socketAddr, remoteAddr)

	//send a status message (including the node's status) to the remote
	header := transport.NewHeader(socketAddr, socketAddr, remoteAddr, 0)
	copyOfLastStatus := n.lastStatus.Freeze()
	transMsg, _ := n.conf.MessageRegistry.MarshalMessage(copyOfLastStatus)

	pktStatus := transport.Packet{Header: &header, Msg: &transMsg}
	errSend := n.conf.Socket.Send(remoteAddr, pktStatus, 0)
	return checkTimeoutError(errSend, 0)
}

func (n *node) SendStatusToRandom(pkt transport.Packet) error {
	socketAddr := n.conf.Socket.GetAddress()
	neighbor := n.routingtable.FindNeighborWithoutContain(socketAddr, pkt.Header.Source)

	if len(neighbor) != 0 {
		chosenNeighbor := neighbor[rand.Int()%(len(neighbor))]
		log.Info().Msgf("[SendStatusToRandom] [%v] => Status [%v]", socketAddr, chosenNeighbor)

		header := transport.NewHeader(socketAddr, socketAddr, chosenNeighbor, 0)
		pktStatus := transport.Packet{Header: &header, Msg: pkt.Msg}

		// Send status to random neighbor
		errSend := n.conf.Socket.Send(chosenNeighbor, pktStatus, 0)
		return checkTimeoutError(errSend, 0)
	}

	return nil

}

func (n *node) SendRumorToRandom(pkt transport.Packet, chosenNeighbor string, msgRumor *types.RumorsMessage) error {
	socketAddr := n.conf.Socket.GetAddress()
	transMsg, errMsg := n.conf.MessageRegistry.MarshalMessage(msgRumor)
	if errMsg != nil {
		return errMsg
	}
	header := transport.NewHeader(socketAddr, socketAddr, chosenNeighbor, 0)

	//pktRumor := transport.Packet{Header: pkt.Header, Msg: &transMsg}
	pktRumor := transport.Packet{Header: &header, Msg: &transMsg}
	log.Info().Msgf("[SendRumorToRandom] [%v] => rumor [%v]", socketAddr, chosenNeighbor)
	errSend := n.conf.Socket.Send(chosenNeighbor, pktRumor, 0)
	//n.ackRecord.UpdateAckChecker(pkt.Header.PacketID, n.conf.AckTimeout)

	err := checkTimeoutError(errSend, 0)
	if err != nil {
		return err
	}

	return nil
}

func (n *node) SendAck(pkt transport.Packet) error {
	// need to change the Src and Dest in header when sending ACK
	newSrc := n.conf.Socket.GetAddress()
	newDest := pkt.Header.Source
	log.Info().Msgf("[SendAck] [%v] => ack [%v]", newSrc, newDest)

	copyOfLastStatus := n.lastStatus.Freeze()
	newMsgAck := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        copyOfLastStatus,
	}
	header := transport.NewHeader(newSrc, newSrc, newDest, 0)

	// Transform AckMessages to Messages
	transMsg, errMsg := n.conf.MessageRegistry.MarshalMessage(newMsgAck)
	if errMsg != nil {
		return errMsg
	}
	pktAct := transport.Packet{Header: &header, Msg: &transMsg}

	// Send back Act to the original sender
	errSend := n.conf.Socket.Send(newDest, pktAct, 0)
	//_ = n.conf.MessageRegistry.ProcessPacket(pktAct)
	return checkTimeoutError(errSend, 0)
}

func (n *node) WaitForAck(pkt transport.Packet, msgRumor *types.RumorsMessage, neighbor []string, chosenN string) {
	if n.conf.AckTimeout != 0 {
		timer := time.NewTimer(n.conf.AckTimeout)
		n.ackRecord.UpdateAckChecker(pkt.Header.PacketID, timer)

		go func() {
			for {
				<-timer.C
				log.Error().Msgf("[WaitForAck] Timeout!!")

				var chosenNeighborNew string
				if len(neighbor) > 1 {
					for {
						chosenNeighborNew = neighbor[rand.Int()%(len(neighbor))]
						if chosenNeighborNew != chosenN {
							break
						}
					}
					log.Info().Msgf("[WaitForAck] Haven Received ack after Timeout, send to another neighbor %v", chosenNeighborNew)
					_ = n.SendRumorToRandom(pkt, chosenNeighborNew, msgRumor)
				}
			}

		}()
	}
}

func (n *node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	msgRumor, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	socketAddr := n.conf.Socket.GetAddress()
	log.Info().Msgf("[ExecRumorsMessage] [%v] => packet [%v] ", pkt.Header.Source, socketAddr)
	isExpected := false

	// Process each rumor
	for _, rumor := range msgRumor.Rumors {
		if _, okk := n.lastStatus.GetandIncrementStatusIfEqual(rumor.Origin, rumor.Sequence); okk {
			//log.Info().Msgf("[ExecRumorsMessage] Expected Rumor")
			isExpected = true

			if socketAddr != rumor.Origin && !Contains(n.routingtable.FindNeighbor(socketAddr), rumor.Origin) {
				log.Info().Msgf("[ExecRumorsMessage] Add [%v]:[%v] to routing table", pkt.Header.Source, pkt.Header.RelayedBy)

				// Should we check if the entry exists before updating the routing table? Or doesn't matter
				n.routingtable.UpdateRoutingtable(rumor.Origin, pkt.Header.RelayedBy)
			}

			n.sentRumor.UpdateRumorMap(rumor.Origin, rumor)

			pktRumor := transport.Packet{
				Header: pkt.Header,
				Msg:    rumor.Msg,
			}

			//log.Info().Msgf("[ExecRumorsMessage] Call ProcessPacket")
			errProcess := n.conf.MessageRegistry.ProcessPacket(pktRumor)
			err := checkTimeoutError(errProcess, 0)
			if err != nil {
				return err
			}
		}
	}

	// Send Ack to the source -> Should send it in the beginning?
	if pkt.Header.Source != socketAddr {
		_ = n.SendAck(pkt)
	}

	if isExpected {
		neighbor := n.routingtable.FindNeighborWithoutContain(socketAddr, pkt.Header.Source)

		if len(neighbor) != 0 {
			chosenNeighbor := neighbor[rand.Int()%(len(neighbor))]

			_ = n.SendRumorToRandom(pkt, chosenNeighbor, msgRumor)

			n.WaitForAck(pkt, msgRumor, neighbor, chosenNeighbor)

			return nil
		}
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

	log.Info().Msgf("[ExecAckMessage] node([%v]) receive AckMessage", n.conf.Socket.GetAddress())
	//socketAddr := n.conf.Socket.GetAddress()
	// stop the timer and process the pkt
	n.ackRecord.FindAckEntryAndStopTimerIfEqual(msgAck.AckedPacketID)

	log.Info().Msgf("[ExecAckMessage] PackedID not in record")

	transMsg, _ := n.conf.MessageRegistry.MarshalMessage(msgAck.Status)
	pktStatus := transport.Packet{Header: pkt.Header, Msg: &transMsg}
	errProcess := n.conf.MessageRegistry.ProcessPacket(pktStatus)
	return checkTimeoutError(errProcess, 0)

}

func (n *node) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {
	msgStatus, okk := msg.(*types.StatusMessage)
	if !okk {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	socketAddr := n.conf.Socket.GetAddress()
	remoteAddr := pkt.Header.Source

	localStatus := n.lastStatus.Freeze()
	remoteStatus := *msgStatus

	remoteHasNewRumor := false
	hasSameView := true
	var rumorToSend []types.Rumor

	// Check if remote has rumors from a source that local doesn't have
	for rk := range remoteStatus {
		remoteSeq := remoteStatus[rk]

		localSeq, ok := localStatus[rk]
		if !ok {
			localSeq = 0
		}
		if remoteSeq > localSeq {

			remoteHasNewRumor = true
			hasSameView = false
			break
		}
	}

	// Check if local has rumors that remote doesn't have
	for lk := range localStatus {
		localSeq := localStatus[lk]

		remoteSeq, ok := remoteStatus[lk]
		if !ok {

			remoteSeq = 0
		}

		if localSeq > remoteSeq {
			rumorToSend = append(rumorToSend, n.sentRumor.ReturnMissingRumorMap(lk, remoteSeq)...)
			hasSameView = false
			//log.Info().Msgf("[ExecStatusMessage] [Case2] get rumorToSend %v", rumorToSend)
		}
	}

	if remoteHasNewRumor {
		log.Info().Msgf("[SyncRemoteStatus] [Case1] [%v] need to send a status message to [%v]", socketAddr, remoteAddr)
		_ = n.SendStatusBackToRemote(remoteAddr)
	}

	if len(rumorToSend) > 0 {
		// Sort rumorToSend by sequence number
		sort.Slice(rumorToSend, func(x, y int) bool { return rumorToSend[x].Sequence < rumorToSend[y].Sequence })
		_ = n.SendMissingRumor(remoteAddr, rumorToSend)
	}

	if hasSameView {
		contMongering := n.conf.ContinueMongering
		tmp := rand.Float64()

		if tmp < contMongering {

			contMongering = float64(1)

		} else {

			contMongering = float64(0)

		}

		switch contMongering {
		case float64(1):
			err := n.SendStatusToRandom(pkt)
			return err
		case float64(0):
			// do nothing -> don't send
			return nil
		}
	}
	return nil
}

func (n *node) ExecEmptyMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	_, ok := msg.(*types.EmptyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msgf("[ExecEmptyMessage] process empty message]")

	return nil

}

func (n *node) ExecPrivateMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	msgPrivate, ok := msg.(*types.PrivateMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msgf("[ExecPrivateMessage] %v ", msgPrivate)
	socketAddr := n.conf.Socket.GetAddress()
	isRecipient := false

	for key := range msgPrivate.Recipients {
		if socketAddr == key {
			isRecipient = true
		}
	}

	switch isRecipient {
	case true:

		pktPrivate := transport.Packet{Header: pkt.Header, Msg: msgPrivate.Msg}
		_ = n.conf.MessageRegistry.ProcessPacket(pktPrivate)
	case false:
		// do nothing
	}

	return nil

}
