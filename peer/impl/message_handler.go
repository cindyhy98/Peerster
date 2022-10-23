package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
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

func (n *node) SendMissingRumor(Dest string, rumor []types.Rumor) error {
	socketAddr := n.conf.Socket.GetAddress()

	log.Info().Msgf("[SendMissingRumor] [%v] => Rumor [%v]", socketAddr, Dest)

	//log.Info().Msgf("[UpdateRumorMap] node(%v), SentRumor = %v", socketAddr, n.sentRumor)

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
	transMsg, _ := n.conf.MessageRegistry.MarshalMessage(n.lastStatus.realLastStatus)

	pktStatus := transport.Packet{Header: &header, Msg: &transMsg}
	errSend := n.conf.Socket.Send(remoteAddr, pktStatus, 0)
	return checkTimeoutError(errSend, 0)
}

func (n *node) SendStatusToRandom(pkt transport.Packet) error {
	socketAddr := n.conf.Socket.GetAddress()
	neighbor := n.routable.FindNeighborWithoutContain(socketAddr, pkt.Header.Source)

	if len(neighbor) != 0 {
		chosenNeighbor := neighbor[rand.Int()%(len(neighbor))]
		log.Info().Msgf("[SendStatusToRandom] [%v] => Status [%v]", socketAddr, chosenNeighbor)

		header := transport.NewHeader(socketAddr, socketAddr, chosenNeighbor, 0)
		pktStatus := transport.Packet{Header: &header, Msg: pkt.Msg}

		// Send status to random neighbor
		errSend := n.conf.Socket.Send(chosenNeighbor, pktStatus, 0)
		return checkTimeoutError(errSend, 0)
	} else {
		log.Error().Msgf("[SendStatusToRandom] No neighbor (Don't need to Send)")
		return nil
	}
}

func (n *node) SendRumorToRandom(pkt transport.Packet, chosenNeighbor string, msgRumor *types.RumorsMessage) error {
	socketAddr := n.conf.Socket.GetAddress()
	transMsg, errMsg := n.conf.MessageRegistry.MarshalMessage(msgRumor)
	if errMsg != nil {
		return errMsg
	}

	//pkt.Header.Source = socketAddr  // TODO: Should I change this?
	pkt.Header.RelayedBy = socketAddr // TODO: Should I change this?
	pkt.Header.Destination = chosenNeighbor
	pkt.Header.TTL -= 1

	pktRumor := transport.Packet{Header: pkt.Header, Msg: &transMsg}

	errSend := n.conf.Socket.Send(pkt.Header.Destination, pktRumor, 0)

	// TODO: record the rumor you've sent (only update the sentRumor after successfully received the ack?)
	n.sentRumor.UpdateRumorMap(pkt.Header.Destination, *msgRumor)

	n.ackRecord.packetID = pkt.Header.PacketID

	err := checkTimeoutError(errSend, 0)
	if err != nil {
		return err
	}
	return nil
}

/*
func (n *node) WaitForAck() bool {
	go func() {
		for {
			select {
			case <-n.ackRecord.T.C:
				log.Error().Msgf("[WaitForAck] Timeout!!")
				return false
			default:
				// without timeout
				// TODO: Should I just return true???
				if !n.ackRecord.received {
					log.Info().Msgf("[WaitForAck] haven't receive ack!")
					return false
				}
				log.Info().Msgf("[WaitForAck] Receive ack!")
				return true
			}
		}
	}()

}
*/

func (n *node) SendAck(pkt transport.Packet) error {
	// need to change the Src and Dest in header when sending ACK
	newSrc := n.conf.Socket.GetAddress()
	newDest := pkt.Header.Source
	log.Info().Msgf("[%v] => ack [%v]", newSrc, newDest)

	newMsgAck := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        n.lastStatus.realLastStatus,
	}
	header := transport.NewHeader(newSrc, newSrc, newDest, 0)

	// Transform AckMessages to Messages
	transMsg, errMsg := n.conf.MessageRegistry.MarshalMessage(newMsgAck)
	if errMsg != nil {
		return errMsg
	}

	pktAct := transport.Packet{Header: &header, Msg: &transMsg}

	// Send back Act to the original sender
	errSend := n.conf.Socket.Send(header.Destination, pktAct, 0)
	//_ = n.conf.MessageRegistry.ProcessPacket(pktAct)
	return checkTimeoutError(errSend, 0)
}

func (n *node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	//pkt.Header.Destination == socketAddr!!

	// cast the message to its actual type. You assume it is the right type.
	msgRumor, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	socketAddr := n.conf.Socket.GetAddress()
	log.Info().Msgf("[ExecRumorsMessage] [%v] => packet [%v] ", pkt.Header.Source, socketAddr)
	isExpected := false
	//toMyself := pkt.Header.Source == socketAddr

	// Process each rumor
	for _, rumor := range msgRumor.Rumors {
		if _, okk := n.lastStatus.GetandIncrementStatusIfEqual(pkt.Header.Source, rumor.Sequence); okk {
			log.Info().Msgf("[ExecRumorsMessage] Expected Rumor")
			isExpected = true

			// TODO: check if the orgSrc is already our neighbor, if not -> add to the routing table
			if !Contains(n.routable.FindNeighbor(socketAddr), pkt.Header.Source) {
				log.Info().Msgf("[ExecRumorsMessage] Add [%v]:[%v] to routing table", pkt.Header.Source, pkt.Header.RelayedBy)

				// TODO: Should we check if the entry exists before updating the routing table? Or doesn't matter
				n.routable.UpdateRoutingtable(pkt.Header.Source, pkt.Header.RelayedBy)
			}

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

	// TODO: Send Ack to the source -> Should send it in the beginning?
	if pkt.Header.Source != socketAddr {
		_ = n.SendAck(pkt)
	}

	// Send RumorMessage to another random neighbor
	if isExpected {
		neighbor := n.routable.FindNeighborWithoutContain(socketAddr, pkt.Header.Source)

		if len(neighbor) != 0 {
			// TODO: Cannot send to the pkt's original src!!
			log.Info().Msgf("[ExecRumorsMessage] all neighbors %v", neighbor)
			chosenNeighbor := neighbor[rand.Int()%(len(neighbor))]

			_ = n.SendRumorToRandom(pkt, chosenNeighbor, msgRumor)

			go func() {
				for {
					select {
					case <-n.ackRecord.T.C:
						log.Error().Msgf("[WaitForAck] Timeout!!")
						var chosenNeighborNew string
						if len(neighbor) > 1 {
							for {
								chosenNeighborNew = neighbor[rand.Int()%(len(neighbor))]
								if chosenNeighborNew != chosenNeighbor {
									break
								}
							}
							log.Info().Msgf("Haven Received ack after Timeout, send to another neighbor %v", chosenNeighborNew)
							_ = n.SendRumorToRandom(pkt, chosenNeighborNew, msgRumor)
						} else {
							log.Info().Msgf("Couldn't find another neighbor")
						}

					default:

					}
				}

			}()

			// TODO: What to return for error?

			return nil
		} else {

			log.Error().Msgf("[ExecRumorsMessage] No neighbor (Don't need to Send)")
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

	log.Info().Msgf("[ExecAckMessage] node([%v]) AckMessage status = %v", n.conf.Socket.GetAddress(), msgAck.Status)
	//socketAddr := n.conf.Socket.GetAddress()
	// TODO: stop the timer and process the pkt

	if msgAck.AckedPacketID == n.ackRecord.packetID {
		//n.ackRecord.received = true
		n.ackRecord.T.Stop()
		log.Info().Msgf("[WaitForAck] Stop! ")
		transMsg, _ := n.conf.MessageRegistry.MarshalMessage(msgAck.Status)
		pktStatus := transport.Packet{Header: pkt.Header, Msg: &transMsg}
		errProcess := n.conf.MessageRegistry.ProcessPacket(pktStatus)
		return checkTimeoutError(errProcess, 0)
	}

	return nil
}

func (n *node) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.

	msgStatus, okk := msg.(*types.StatusMessage)
	if !okk {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	socketAddr := n.conf.Socket.GetAddress()
	remoteAddr := pkt.Header.Source

	localStatus := n.lastStatus.realLastStatus
	remoteStatus := *msgStatus
	log.Info().Msgf("[ExecStatusMessage] localAddr = %v, localStatus = %v, remoteAddr = %v , remoteStatus = ", socketAddr, localStatus, remoteAddr, remoteStatus)

	remoteHasNewRumor := false
	hasSameView := true
	var rumorToSend []types.Rumor

	// Check if remote has rumors from a source that local doesn't have
	for rk := range remoteStatus {
		//if rk == remoteAddr {
		//	continue
		//}

		remoteSeq := remoteStatus[rk]

		// TODO: [Case 1]: Remote peer has new message -> local peer need to send a status message to remote
		// -> 1. Key only exists in remoteStatus, not in localStatus
		// -> 2. If Key exists in both cases, check if remoteSeq is higher than localSeq
		localSeq, ok := localStatus[rk]
		if !ok {
			localSeq = 0
		}
		if remoteSeq > localSeq {
			log.Info().Msgf("[SyncRemoteStatus] [Case1]: message [%v]:[%v] only in remote peer", rk, remoteSeq)
			remoteHasNewRumor = true
			hasSameView = false
			break
		}
	}

	// Check if local has rumors that remote doesn't have
	for lk := range localStatus {
		//if lk == socketAddr {
		//	continue
		//}

		localSeq := localStatus[lk]
		//if localSeq == 0 {
		//	continue
		//}
		// TODO: [Case 2]: Local peer has new message -> local peer need to send all missing rumors (put them all in a big slice) to remote
		// -> 1. Key only exists in localStatus, not in remoteStatus
		// -> 2. If Key exists in both cases, check if localSeq is higher than remoteSeq
		remoteSeq, ok := remoteStatus[lk]
		if !ok {
			log.Info().Msgf("[ExecStatusMessage] [Case2] Couldn't find key(%v) in remoteStatus %v", lk, remoteStatus)
			remoteSeq = 0
		}

		if localSeq > remoteSeq {
			log.Info().Msgf("[ExecStatusMessage] Different key(%v), localSeq = %v, remoteSeq = %v", lk, localSeq, remoteSeq)

			for _, r := range n.sentRumor.ReturnMissingRumorMap(lk, remoteSeq) {
				rumorToSend = append(rumorToSend, r)
			}
			hasSameView = false
			log.Info().Msgf("[ExecStatusMessage] [Case2] get rumorToSend %v", rumorToSend)
		}
	}

	if remoteHasNewRumor {
		_ = n.SendStatusBackToRemote(remoteAddr)
	}

	if len(rumorToSend) > 0 {
		_ = n.SendMissingRumor(remoteAddr, rumorToSend)
		if remoteHasNewRumor {
			log.Info().Msgf("[ExecStatusMessage] [Case3] Do both things")
		}
	}

	if hasSameView {
		contMongering := n.conf.ContinueMongering

		log.Info().Msgf("[ExecStatusMessage] [Case4] Same View, [Before] ContinueMongering = %v", contMongering)
		if contMongering == 0.5 {
			contMongering = float64(rand.Int() % 2)
		}
		log.Info().Msgf("[ExecStatusMessage] [Case4] Same View, [After] ContinueMongering = %v", contMongering)

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

	log.Info().Msgf("[ExecEmptyMessage] process empty message....pkt src = %v, dst = %v, msg = %v", pkt.Header.Source, pkt.Header.Destination, pkt.Msg)

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
		// TODO: is this right?

		pktPrivate := transport.Packet{Header: pkt.Header, Msg: msgPrivate.Msg}
		_ = n.conf.MessageRegistry.ProcessPacket(pktPrivate)
	case false:
		// do nothing
	}

	return nil

}
