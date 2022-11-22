package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"strconv"
	"sync"
	"time"
)

type safePaxosInstance struct {
	*sync.Mutex
	maxID               uint
	currentLogicalClock uint
	phase               int
	promises            []*types.PaxosPromiseMessage
	acceptedID          uint
	acceptedValue       *types.PaxosValue
	proposedValue       *types.PaxosValue
}

func (p *safePaxosInstance) GetPromises() []*types.PaxosPromiseMessage {
	p.Lock()
	defer p.Unlock()

	return p.promises
}

func (p *safePaxosInstance) AddPromises(peerPromise *types.PaxosPromiseMessage) {
	p.Lock()
	defer p.Unlock()

	p.promises = append(p.promises, peerPromise)
}

func (n *node) FindAcceptedValueInPromises(promises []*types.PaxosPromiseMessage, proposedValue types.PaxosValue) types.PaxosValue {
	// Suppose that the promise is not nil
	numberOfAcceptedValue := 0

	// Find the correct AcceptedID and AcceptedValue by checking all promises
	maxAcceptedID := uint(0)
	selectedIndex := 0
	for i, promise := range promises {
		if promise.AcceptedValue == nil {
			numberOfAcceptedValue += 1
			continue
		}
		if promise.AcceptedID > maxAcceptedID {
			maxAcceptedID = promise.AcceptedID
			selectedIndex = i
		}
	}

	if numberOfAcceptedValue == len(promises) {
		log.Info().Msgf("[FindAcceptedValueInPromises] no PaxosPromiseMessage contained an AcceptedValue")
		return proposedValue
	}

	//p.acceptedID = p.promises[selectedIndex].AcceptedID
	//p.acceptedValue = p.promises[selectedIndex].AcceptedValue

	return *promises[selectedIndex].AcceptedValue
}

func (n *node) BroadcastPaxosPrepare(ID uint) {
	log.Info().Msgf("[BroadcastPaxosPrepare] ID = %v", ID)
	// Broadcast a PaxosPrepareMessage
	go func() {

		// Update paxos phase to 1
		n.paxosInstance.phase = 1

		newPaxosPrepareMessage := types.PaxosPrepareMessage{
			Step:   uint(0),
			ID:     ID,
			Source: n.conf.Socket.GetAddress(),
		}

		transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newPaxosPrepareMessage)
		log.Info().Msgf("[BroadcastPaxosPrepare] [%v] Paxos Prepare => everyone", n.conf.Socket.GetAddress())

		_ = n.Broadcast(transMsg)
	}()
}

func (n *node) BroadcastPaxosPropose(promises []*types.PaxosPromiseMessage, proposedValue types.PaxosValue) {

	go func() {
		// Update paxos phase to 2
		n.paxosInstance.phase = 2

		// Broadcast a PaxosProposeMessage
		newPaxosProposeMessage := types.PaxosProposeMessage{
			Step:  n.paxosInstance.currentLogicalClock,
			ID:    n.paxosInstance.maxID,
			Value: n.FindAcceptedValueInPromises(promises, proposedValue),
		}

		transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newPaxosProposeMessage)
		log.Info().Msgf("[BroadcastPaxosPropose] [%v] Paxos Propose => everyone", n.conf.Socket.GetAddress())

		_ = n.Broadcast(transMsg)
	}()

}

func (n *node) WaitForPaxosPromise(timeout time.Duration, channel chan types.Message) (bool, []*types.PaxosPromiseMessage) {
	reachThreshold := false
	totalPromiseMsg := make([]*types.PaxosPromiseMessage, 0)

	for hasTimeout := false; !hasTimeout; {
		select {
		case msg := <-channel:
			msgPaxosPromise, _ := msg.(*types.PaxosPromiseMessage)

			log.Info().Msgf("[WaitForPaxosPromise] get promise data %v ", msgPaxosPromise)
			if len(totalPromiseMsg) < n.conf.PaxosThreshold(n.conf.TotalPeers) {
				totalPromiseMsg = append(totalPromiseMsg, msgPaxosPromise)
			} else {
				log.Info().Msgf("[WaitForPaxosPromise] reach the threshold")
				reachThreshold = true

				break

			}

		case <-time.After(timeout):
			log.Info().Msgf("[WaitForPaxosPromise] timeout reaches")
			hasTimeout = true
		}
	}

	return reachThreshold, totalPromiseMsg
}

//func (n *node) GroupPaxosValue()

// TODO: need to change this function
func (n *node) WaitForPaxosAccept(timeout time.Duration, channel chan types.Message) (bool, []types.PaxosValue) {
	reachThreshold := false
	totalAcceptMsg := make([]types.PaxosValue, 0)

	for hasTimeout := false; !hasTimeout; {
		select {
		case msg := <-channel:
			msgPaxosAccept, _ := msg.(*types.PaxosAcceptMessage)

			log.Info().Msgf("[WaitForPaxosAccept] get accept data %v ", msgPaxosAccept)
			//if len(totalAcceptMsg) < n.conf.PaxosThreshold(n.conf.TotalPeers) {
			//	totalAcceptMsg = append(totalAcceptMsg, msgPaxosAccept)
			//} else {
			//
			//	log.Info().Msgf("[WaitForPaxosAccept] reach the threshold")
			//	reachThreshold = true
			//}

		case <-time.After(timeout):
			log.Info().Msgf("[WaitForPaxosAccept] timeout reaches")
			hasTimeout = true
		}
	}

	return reachThreshold, totalAcceptMsg
}

func (n *node) RunPaxosProposer(proposedValue types.PaxosValue) (types.PaxosValue, error) {
	decidedValue := proposedValue

	// [Phase 1]
	n.BroadcastPaxosPrepare(n.conf.PaxosID)

	// Wait for promise
	promiseKey := n.ComputePaxosChannelKey("promise", n.conf.PaxosID)
	promiseChannel := n.paxosMsgChannel.GetPaxosMsgChannel(promiseKey)

	reachPromiseThreshold, totalPromiseMsg := n.WaitForPaxosPromise(n.conf.PaxosProposerRetry, promiseChannel)
	log.Info().Msgf("[RunPaxosProposer] reachThreshold = %v, totalPromiseMsg = %v", reachPromiseThreshold, totalPromiseMsg)
	for !reachPromiseThreshold {
		// retry with the nextID
		n.BroadcastPaxosPrepare(n.conf.PaxosID + n.conf.TotalPeers)
	}

	// Reach the threshold
	// delete the channel
	log.Info().Msgf("[RunPaxosProposer] Delete promise channel")
	n.paxosMsgChannel.DeletePaxosMsgChannel(promiseKey)

	// [Phase 2]
	n.BroadcastPaxosPropose(totalPromiseMsg, proposedValue)

	// Wait for promise
	acceptKey := n.ComputePaxosChannelKey("accept", n.conf.PaxosID)
	acceptChannel := n.paxosMsgChannel.GetPaxosMsgChannel(acceptKey)

	reachAcceptThreshold, acceptedValue := n.WaitForPaxosAccept(n.conf.PaxosProposerRetry, acceptChannel)
	log.Info().Msgf("[RunPaxosProposer] reachThreshold = %v, totalPromiseMsg = %v", reachAcceptThreshold, acceptedValue)

	// delete the channel
	log.Info().Msgf("[RunPaxosProposer] Delete accept channel")
	n.paxosMsgChannel.DeletePaxosMsgChannel(acceptKey)

	return decidedValue, nil
}

func (n *node) ComputePaxosChannelKey(msgType string, id uint) string {
	return n.conf.Socket.GetAddress() + "-" + msgType + "-" + strconv.Itoa(int(id))
}

func (n *node) Prepare(channel chan types.Message) error {
	for received := false; !received; {
		select {
		case msg := <-channel:
			received = true
			msgPaxosPrepare, ok := msg.(*types.PaxosPrepareMessage)
			if !ok {
				return xerrors.Errorf("wrong type: %T", msg)
			}

			log.Info().Msgf("[Prepare] received msgPaxosPrepare %v", msgPaxosPrepare)
			// [Task1]
			if n.CheckPaxosPrepareMessage(msgPaxosPrepare) {
				// Ignore it
			} else {
				// Update paxos phase to 1
				n.paxosInstance.phase = 1

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
				newPrivateMessage := types.PrivateMessage{
					Recipients: recipients,
					Msg:        &transMsg,
				}
				transMsgBroadcast, _ := n.conf.MessageRegistry.MarshalMessage(newPrivateMessage)

				err := n.Broadcast(transMsgBroadcast)
				return err
			}
		default:
			log.Info().Msgf("Haven't received")
		}
	}

	return nil
}

func (n *node) Promise(channel chan types.Message) error {
	return nil
}

func (n *node) Propose(channel chan types.Message) error {

	select {
	case msg := <-channel:
		msgPaxosPropose, ok := msg.(*types.PaxosProposeMessage)
		if !ok {
			return xerrors.Errorf("wrong type: %T", msg)
		}
		if n.CheckPaxosProposeMessage(msgPaxosPropose) {
			// Ignore
		} else {
			// Question: should I change it here?
			//n.paxosInstance.acceptedID = msgPaxosPropose.ID
			//n.paxosInstance.acceptedValue = &msgPaxosPropose.Value

			newPaxosAcceptMessage := types.PaxosAcceptMessage{
				Step:  msgPaxosPropose.Step,
				ID:    msgPaxosPropose.ID,
				Value: msgPaxosPropose.Value,
			}
			transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newPaxosAcceptMessage)
			err := n.Broadcast(transMsg)
			return err
		}

	}
	return nil
}

func (n *node) Accept(channel chan types.Message) error {
	select {
	case msg := <-channel:
		msgPaxosAccept, ok := msg.(*types.PaxosAcceptMessage)
		if !ok {
			return xerrors.Errorf("wrong type: %T", msg)
		}

		log.Info().Msgf("[Accept] %v", msgPaxosAccept)
	}

	return nil
}
