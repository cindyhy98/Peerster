package impl

import (
	"bytes"
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
	"regexp"
	"sort"
	"time"
)

/* Supplement functions for Message Handler */

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

func (n *node) SearchForMatchFile(pattern string) []types.FileInfo {
	// convert pattern string to a regexp.Regexp
	reg := regexp.MustCompile(pattern)
	filesInfo := make([]types.FileInfo, 0)

	n.conf.Storage.GetNamingStore().ForEach(func(name string, metahash []byte) bool {
		switch reg.MatchString(name) {
		case true:
			//log.Info().Msgf("[SearchForMatchFile] match file name(%v)", name)
			// Find Metafile from local
			metafile := n.conf.Storage.GetDataBlobStore().Get(string(metahash))
			if metafile == nil {
				//log.Info().Msgf("[SearchForMatchFile] match files name(%v) but metafile is empty", name)
				return true
			}

			chunksFile := make([][]byte, 0)
			metafileTrim := bytes.Split(metafile, []byte(peer.MetafileSep))
			for _, chunkHashHex := range metafileTrim {
				//log.Info().Msgf("[SearchForMatchFile] chunk hash (%v)", string(chunkHashHex))
				chunk := n.conf.Storage.GetDataBlobStore().Get(string(chunkHashHex))
				if chunk == nil {
					chunksFile = append(chunksFile, nil)
				} else {
					chunksFile = append(chunksFile, chunkHashHex)
				}
			}
			//log.Info().Msgf("[SearchForMatchFile] local chunksFile = %v", chunksFile)

			file := types.FileInfo{
				Name:     name,
				Metahash: string(metahash),
				Chunks:   chunksFile,
			}
			filesInfo = append(filesInfo, file)
		case false:
			// Do nothing
			//log.Info().Msgf("[SearchForMatchFile] doesn't match the file name")
		}

		return true
	})

	log.Info().Msgf("[SearchForMatchFile] in node(%v) filesInfo = %v", n.conf.Socket.GetAddress(), filesInfo)
	return filesInfo
}

func (n *node) ForwardSearchRequest(searchReq *types.SearchRequestMessage, neighbors []string) error {

	//neighbors := n.routingtable.FindNeighbor(n.conf.Socket.GetAddress())

	remainingBudget := searchReq.Budget - 1

	shuffleNeighbors, budgetPerNeighbor := n.FindBudgetPerNeighbor(neighbors, remainingBudget)

	if len(shuffleNeighbors) != 0 && remainingBudget != 0 {
		switch budgetPerNeighbor {
		case 0:
			for i := 0; i < int(remainingBudget); i++ {

				log.Info().Msgf("[ForwardSearchRequest] [%v] searchReq => [%v]", n.conf.Socket.GetAddress(), shuffleNeighbors[i])
				_ = n.SendSearchRequest(searchReq.Pattern, uint(1), shuffleNeighbors[i], searchReq.RequestID, searchReq.Origin)

			}
		default:
			for i := 0; i < len(shuffleNeighbors); i++ {

				if i == len(shuffleNeighbors)-1 {
					// last one -> budget should be the rest
					usedBudget := uint(len(shuffleNeighbors)-1) * budgetPerNeighbor
					budgetPerNeighbor = remainingBudget - usedBudget
				}

				log.Info().Msgf("[ForwardSearchRequest] [%v] searchReq => [%v]", n.conf.Socket.GetAddress(), shuffleNeighbors[i])
				_ = n.SendSearchRequest(searchReq.Pattern, budgetPerNeighbor,
					shuffleNeighbors[i], searchReq.RequestID, searchReq.Origin)

			}

		}

	}

	return nil
}

func (n *node) CheckPaxosPrepareMessage(msgPaxosPrepare *types.PaxosPrepareMessage) bool {
	shouldIgnore := false
	// Ignore messages whose Step field does not match your current logical clock
	if msgPaxosPrepare.Step != n.paxosInstance.currentLogicalClock {
		shouldIgnore = true
		log.Info().Msgf("Ignore old paxos prepare message %v %v", n.paxosInstance.currentLogicalClock, msgPaxosPrepare.Step)

	}

	if msgPaxosPrepare.ID <= n.paxosInstance.maxID {
		shouldIgnore = true
		log.Info().Msgf("Ignore a prepare message as Paxos ID (%v) isn't greater than the maxID (%v)", msgPaxosPrepare.ID, n.paxosInstance.maxID)
	} else {
		log.Info().Msgf("[CheckPaxosPrepareMessage] Update maxID to %v in prepare phase", msgPaxosPrepare.ID)
		n.paxosInstance.maxID = msgPaxosPrepare.ID
	}

	return shouldIgnore
}

func (n *node) CheckPaxosPromiseMessage(msgPaxosPromise *types.PaxosPromiseMessage) bool {
	shouldIgnore := false

	// Ignore messages whose Step field does not match your current logical clock
	if msgPaxosPromise.Step != n.paxosInstance.currentLogicalClock {
		shouldIgnore = true
		log.Info().Msgf("Ignore old paxos promise message %v %v", n.paxosInstance.currentLogicalClock, msgPaxosPromise.Step)

	}

	//// Ignore messages if the proposer is not in Paxos phase 1
	//if n.paxosInstance.currentPhase != 1 {
	//	shouldIgnore = true
	//	log.Info().Msgf("Ignore a promise message as Paxos is in Phase [%v]", n.paxosInstance.currentPhase)
	//}

	return shouldIgnore
}

func (n *node) CheckPaxosProposeMessage(msgPaxosPropose *types.PaxosProposeMessage) bool {
	shouldIgnore := false
	// Ignore messages whose Step field does not match your current logical clock
	if msgPaxosPropose.Step != n.paxosInstance.currentLogicalClock {
		shouldIgnore = true
		log.Info().Msgf("Ignore old paxos propose message %v %v", n.paxosInstance.currentLogicalClock, msgPaxosPropose.Step)

	}

	if msgPaxosPropose.ID != n.paxosInstance.maxID {
		shouldIgnore = true
		log.Info().Msgf("Ignore a propose message as Paxos ID (%v) isn't equal the maxID (%v)", msgPaxosPropose.ID, n.paxosInstance.maxID)
	}

	return shouldIgnore
}

func (n *node) CheckPaxosAcceptMessage(msgPaxosAccept *types.PaxosAcceptMessage) bool {
	shouldIgnore := false

	// Ignore messages whose Step field does not match your current logical clock
	if msgPaxosAccept.Step != n.paxosInstance.currentLogicalClock {
		shouldIgnore = true
		log.Info().Msgf("Ignore old paxos accept message %v %v", n.paxosInstance.currentLogicalClock, msgPaxosAccept.Step)

	}

	//// Ignore messages if the proposer is not in Paxos phase 1
	//if n.paxosInstance.currentPhase != 2 {
	//	shouldIgnore = true
	//	log.Info().Msgf("Ignore a accept message as Paxos is in Phase [%v]", n.paxosInstance.currentPhase)
	//}

	return shouldIgnore
}

func (n *node) BroadcastPaxosPromise(msgPaxosPrepare *types.PaxosPrepareMessage) {

	go func() {
		// Response with a PaxosPromiseMessage inside PrivateMessage
		// In prepared step -> don't change the AcceptedID
		newPaxosPromiseMessage := types.PaxosPromiseMessage{
			Step:          msgPaxosPrepare.Step, // current TLC identifier
			ID:            msgPaxosPrepare.ID,   // proposer ID
			AcceptedID:    n.paxosInstance.acceptedID,
			AcceptedValue: n.paxosInstance.acceptedValue,
		}

		transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newPaxosPromiseMessage)

		recipients := make(map[string]struct{}, 0)
		recipients[msgPaxosPrepare.Source] = struct{}{}
		//log.Info().Msgf("[ExecPaxosPrepareMessage] recipients = %v, should contain %v", recipients, msgPaxosPrepare.Source)
		newPrivateMessage := types.PrivateMessage{
			Recipients: recipients,
			Msg:        &transMsg,
		}
		transMsgBroadcast, _ := n.conf.MessageRegistry.MarshalMessage(newPrivateMessage)
		log.Info().Msgf("[%v] Private (Paxos Promise) => everyone", n.conf.Socket.GetAddress())

		n.paxosPromiseMajority.InitNotifier(msgPaxosPrepare.Step)
		_ = n.Broadcast(transMsgBroadcast)

	}()

}

func (n *node) BroadcastPaxosAccept(msgPaxosPropose *types.PaxosProposeMessage) {
	// Question: should I change it here?
	// Need to change this later
	go func() {
		newPaxosAcceptMessage := types.PaxosAcceptMessage{
			Step:  msgPaxosPropose.Step,
			ID:    msgPaxosPropose.ID,
			Value: msgPaxosPropose.Value,
		}
		transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newPaxosAcceptMessage)
		log.Info().Msgf("[%v] Paxos Accept => everyone", n.conf.Socket.GetAddress())

		n.paxosAcceptMajority.InitNotifier(msgPaxosPropose.Step)
		_ = n.Broadcast(transMsg)
	}()

}

/* Exec Message Handler */

func (n *node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	msgRumor, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	socketAddr := n.conf.Socket.GetAddress()
	log.Info().Msgf("[ExecRumorsMessage] [%v] => packet [%v]", pkt.Header.Source, socketAddr)
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

			log.Info().Msgf("[ExecRumorsMessage] Call ProcessPacket")
			errProcess := n.conf.MessageRegistry.ProcessPacket(pktRumor)
			err := checkTimeoutError(errProcess, 0)
			if err != nil {
				log.Error().Msgf("[ExecRumorsMessage] %v", err)
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
		log.Info().Msgf("[ExecPrivateMessage] process private message (%v => %v)", pkt.Header.Source, pkt.Header.Destination)
		_ = n.conf.MessageRegistry.ProcessPacket(pktPrivate)
	case false:
		// do nothing
	}

	return nil

}

func (n *node) ExecDataRequestMessage(msg types.Message, pkt transport.Packet) error {
	msgDataRequest, ok := msg.(*types.DataRequestMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msgf("[ExecDataRequestMessage] %v ", msgDataRequest)

	// send back a reply message
	newDataReplyMsg := types.DataReplyMessage{
		RequestID: msgDataRequest.RequestID,
		Key:       msgDataRequest.Key,
		Value:     n.conf.Storage.GetDataBlobStore().Get(msgDataRequest.Key),
	}

	transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newDataReplyMsg)
	log.Info().Msgf("[ExecDataRequestMessage] [%v] DataReply => [%v] ", n.conf.Socket.GetAddress(), pkt.Header.Source)
	err := n.Unicast(pkt.Header.Source, transMsg)

	if err != nil {
		log.Error().Msgf("[ExecDataRequestMessage Error] %v", err)
		return err
	}
	// should call Unicast to send back the data!
	return nil
}

func (n *node) ExecDataReplyMessage(msg types.Message, pkt transport.Packet) error {
	msgDataReply, ok := msg.(*types.DataReplyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msgf("[ExecDataReplyMessage] %v ", msgDataReply)

	// store chunk in local
	if msgDataReply.Value != nil {

		n.dataReply.UpdateDataReplyEntry(msgDataReply.RequestID, msgDataReply.Value)
		n.conf.Storage.GetDataBlobStore().Set(msgDataReply.Key, msgDataReply.Value)

		log.Info().Msgf("[ExecDataReplyMessage] store chunk of key %v in local storage", msgDataReply.Key)
		return nil
	}
	return errors.New("entry not found")

}

func (n *node) ExecSearchRequestMessage(msg types.Message, pkt transport.Packet) error {
	msgSearchRequest, ok := msg.(*types.SearchRequestMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	//log.Info().Msgf("[ExecSearchRequestMessage] %v ", msgSearchRequest)

	// Forward the searchRequest if budget permits
	neighbors := n.routingtable.FindNeighborWithoutContain(n.conf.Socket.GetAddress(), pkt.Header.Source)
	_ = n.ForwardSearchRequest(msgSearchRequest, neighbors)

	newSearchReplyMsg := types.SearchReplyMessage{
		RequestID: msgSearchRequest.RequestID,
		Responses: n.SearchForMatchFile(msgSearchRequest.Pattern),
	}

	transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newSearchReplyMsg)

	if msgSearchRequest.Origin != pkt.Header.Source {
		// this is a forwarded searchReq
		header := transport.NewHeader(
			n.conf.Socket.GetAddress(), // source
			n.conf.Socket.GetAddress(), // relay
			msgSearchRequest.Origin,    // destination
			0,                          // TTL
		)
		pktNew := transport.Packet{
			Header: &header,
			Msg:    &transMsg,
		}
		//log.Info().Msgf("[ExecSearchRequestMessage] [%v] Forwarded SearchReply => [%v] ",
		//	n.conf.Socket.GetAddress(), pkt.Header.Source)
		err := n.conf.Socket.Send(pkt.Header.Source, pktNew, 0)
		if err != nil {
			log.Error().Msgf("[ExecSearchRequestMessage Error] %v", err)
			return err
		}
	} else {
		// this is the original searchReq

		//log.Info().Msgf("[ExecSearchRequestMessage] [%v] Original SearchReply => [%v] ",
		//	n.conf.Socket.GetAddress(), pkt.Header.Source)
		err := n.Unicast(pkt.Header.Source, transMsg)

		if err != nil {
			log.Error().Msgf("[ExecSearchRequestMessage Error] %v", err)
			return err
		}
	}

	return nil
}

func (n *node) ExecSearchReplyMessage(msg types.Message, pkt transport.Packet) error {
	msgSearchReply, ok := msg.(*types.SearchReplyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msgf("[ExecSearchReplyMessage] %v ", msgSearchReply)

	// store naming in local
	if msgSearchReply.Responses != nil {
		//allFileName := make([]string, 0)
		for _, file := range msgSearchReply.Responses {

			//allFileName = append(allFileName, file.Name)
			// Update the naming store
			log.Info().Msgf("[ExecSearchReplyMessage] Store %v:%v in NamingStore", file.Name, file.Metahash)
			n.conf.Storage.GetNamingStore().Set(file.Name, []byte(file.Metahash))

			// Update catalog
			//log.Info().Msgf("[ExecSearchReplyMessage] Original catalog = %v", n.GetCatalog())
			n.UpdateCatalog(file.Metahash, pkt.Header.Source)
			for _, chunkhash := range file.Chunks {
				// Question: is the peer addr correct??
				if chunkhash != nil {
					n.UpdateCatalog(string(chunkhash), pkt.Header.Source)
				}

			}
			//log.Info().Msgf("[ExecSearchReplyMessage] Updated catalog = %v", n.GetCatalog())

		}

		n.searchReply.UpdateSearchReplyEntry(msgSearchReply.RequestID, msgSearchReply.Responses)
	}

	return nil
}

func (n *node) ExecPaxosPrepareMessage(msg types.Message, pkt transport.Packet) error {

	msgPaxosPrepare, ok := msg.(*types.PaxosPrepareMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msgf("[%v] <= Paxos Prepare (%v) [%v]", n.conf.Socket.GetAddress(), msgPaxosPrepare, pkt.Header.Source)

	if n.CheckPaxosPrepareMessage(msgPaxosPrepare) {
		// Ignore it
		return nil
	}

	n.BroadcastPaxosPromise(msgPaxosPrepare)
	return nil
}

func (n *node) ExecPaxosPromiseMessage(msg types.Message, pkt transport.Packet) error {

	msgPaxosPromise, ok := msg.(*types.PaxosPromiseMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msgf("[%v] <= Paxos Promise (%v) [%v]", n.conf.Socket.GetAddress(), msgPaxosPromise, pkt.Header.Source)

	if n.CheckPaxosPromiseMessage(msgPaxosPromise) {
		// Ignore it
		return nil
	}

	go func() {
		// Collect PaxosPromiseMessage Until a threshold
		n.paxosPromiseMajority.UpdateCounter(msgPaxosPromise.Step, "")
		n.paxosInstance.UpdatePaxosPromises(msgPaxosPromise)
		if n.paxosPromiseMajority.GetCounter(msgPaxosPromise.Step, "") >=
			n.conf.PaxosThreshold(n.conf.TotalPeers) {

			// Send a notification to unblock
			n.paxosPromiseMajority.UpdateNotifier(msgPaxosPromise.Step, true)
			return
		}
	}()

	return nil
}

func (n *node) ExecPaxosProposeMessage(msg types.Message, pkt transport.Packet) error {
	msgPaxosPropose, ok := msg.(*types.PaxosProposeMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	log.Info().Msgf("[%v] <= Paxos Propose (%v) [%v]", n.conf.Socket.GetAddress(), msgPaxosPropose, pkt.Header.Source)

	if n.CheckPaxosProposeMessage(msgPaxosPropose) {
		// Ignore
		return nil
	}

	// this PaxosPropose is the one with maxID -> store its value
	// Store the AcceptedID and AcceptedValue in paxosInstance (It's yours accepted value)
	n.paxosInstance.UpdatePaxosAcceptedIDAndAcceptedValue(msgPaxosPropose)

	n.BroadcastPaxosAccept(msgPaxosPropose)
	return nil
}

func (n *node) ExecPaxosAcceptMessage(msg types.Message, pkt transport.Packet) error {
	msgPaxosAccept, ok := msg.(*types.PaxosAcceptMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msgf("[%v] <= Paxos Accept (%v) [%v]", n.conf.Socket.GetAddress(), msgPaxosAccept, pkt.Header.Source)

	if n.CheckPaxosAcceptMessage(msgPaxosAccept) {
		// Ignore
		return nil
	}

	//if n.conf.Socket.GetAddress() == pkt.Header.Source {
	//	return nil
	//}
	go func() {
		n.paxosAcceptMajority.UpdateCounter(msgPaxosAccept.Step, msgPaxosAccept.Value.UniqID)
		//n.paxosInstance.UpdatePaxosPromises(msgPaxosPromise)
		log.Info().Msgf("[ExecPaxosAcceptMessage] paxosThreshold = %v", n.conf.PaxosThreshold(n.conf.TotalPeers))
		if n.paxosAcceptMajority.GetCounter(msgPaxosAccept.Step, msgPaxosAccept.Value.UniqID) >=
			n.conf.PaxosThreshold(n.conf.TotalPeers) {

			log.Info().Msgf("[ExecPaxosAcceptMessage] [%v] paxosInstance AcceptedID %v, AcceptedValue %v",
				n.conf.Socket.GetAddress(), n.paxosInstance.acceptedID, n.paxosInstance.acceptedValue)
			// Send a notification to unblock
			n.paxosAcceptMajority.UpdateNotifier(msgPaxosAccept.Step, true)
			return
		}
	}()

	return nil
}
