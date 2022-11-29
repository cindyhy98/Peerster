package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/types"
	"time"
)

func (n *node) BroadcastPaxosPrepare(ID uint) {

	go func() {
		log.Info().Msgf("[BroadcastPaxosPrepare] ID = %v", ID)
		// Broadcast a PaxosPrepareMessage
		newPaxosPrepareMessage := types.PaxosPrepareMessage{
			Step:   n.tlcCurrentState.currentLogicalClock,
			ID:     ID,
			Source: n.conf.Socket.GetAddress(),
		}

		transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newPaxosPrepareMessage)
		log.Info().Msgf("[BroadcastPaxosPrepare] [%v] Paxos Prepare => everyone", n.conf.Socket.GetAddress())

		_ = n.Broadcast(transMsg)
	}()

}

func (n *node) WaitForPaxosPromise(timeout time.Duration) bool {
	notifier := false
	notifierChannel := n.paxosPromiseMajority.InitNotifier(n.tlcCurrentState.currentLogicalClock)
	defer n.paxosPromiseMajority.DeleteNotifier(n.tlcCurrentState.currentLogicalClock)

	select {
	case notifier = <-notifierChannel:
		// Reach a majority of PaxosPromises -> progress to phase two
		log.Info().Msgf("[WaitForPaxosPromise] get a majority of promises")
		break
	case <-time.After(timeout):
		log.Info().Msgf("[WaitForPaxosPromise] timeout reaches, need to retry from phase 1")

	}

	return notifier

}

func (n *node) EnterPhaseOne() bool {
	reachPromiseMajority := false

	Id := n.conf.PaxosID + n.paxosCurrentState.offsetID*n.conf.TotalPeers
	n.BroadcastPaxosPrepare(Id)

	// Wait for a majority of PaxosPromise
	reachPromiseMajority = n.WaitForPaxosPromise(n.conf.PaxosProposerRetry)

	if !reachPromiseMajority {
		// For Retry after timeout
		n.paxosCurrentState.UpdatePaxosOffsetID()
	}

	return reachPromiseMajority

}

func (n *node) BroadcastPaxosPropose(proposedValue types.PaxosValue) {

	go func() {
		// Broadcast a PaxosProposeMessage
		newPaxosProposeMessage := types.PaxosProposeMessage{
			Step:  n.tlcCurrentState.currentLogicalClock,
			ID:    n.paxosCurrentState.maxID,
			Value: n.paxosCurrentState.FindAcceptedValueInPaxosPromises(proposedValue),
		}

		transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newPaxosProposeMessage)
		log.Info().Msgf("[BroadcastPaxosPropose] [%v] Paxos Propose => everyone", n.conf.Socket.GetAddress())

		_ = n.Broadcast(transMsg)
	}()

}

func (n *node) WaitForPaxosAccept(timeout time.Duration) bool {
	notifier := false
	notifierChannel := n.paxosAcceptMajority.InitNotifier(n.tlcCurrentState.currentLogicalClock)
	defer n.paxosAcceptMajority.DeleteNotifier(n.tlcCurrentState.currentLogicalClock)

	select {
	case notifier = <-notifierChannel:
		// Reach a majority of PaxosPromises -> progress to phase two
		log.Info().Msgf("[WaitForPaxosAccept] get a majority of accept")
		break
	case <-time.After(timeout):
		log.Info().Msgf("[WaitForPaxosAccept] timeout reaches, need to retry from phase 1")

	}

	return notifier
}

func (n *node) EnterPhaseTwo(proposedValue types.PaxosValue) bool {
	reachAcceptMajority := false
	n.BroadcastPaxosPropose(proposedValue)

	// Wait for a majority of PaxosPromise
	reachAcceptMajority = n.WaitForPaxosAccept(n.conf.PaxosProposerRetry)

	if !reachAcceptMajority {
		// For Retry after timeout
		n.paxosCurrentState.UpdatePaxosOffsetID()
	}

	return reachAcceptMajority

}

func (n *node) WaitForTLC() bool {
	notifier := false
	notifierChannel := n.tlcMajority.InitNotifier(n.tlcCurrentState.currentLogicalClock)
	defer n.tlcMajority.DeleteNotifier(n.tlcCurrentState.currentLogicalClock)

	log.Debug().Msgf("TLC %v", notifierChannel)
	select {
	case notifier = <-notifierChannel:
		// Reach a majority of PaxosPromises -> progress to phase two
		log.Info().Msgf("[WaitForTLC] get a majority of TLC")
	}

	return notifier
}

func (n *node) EnterPhaseTLC() bool {
	return n.WaitForTLC()
}

func (n *node) RunPaxos(proposedValue types.PaxosValue) (types.PaxosValue, error) {

	decidedValue := proposedValue
	log.Info().Msgf("[RunPaxos] Before running Paxos decidedValue = %v", decidedValue)

	reachMajority := false
	// [Phase 1]
	for !reachMajority {
		reachMajority = n.EnterPhaseOne()
		log.Info().Msgf("[RunPaxos] reachMajority in phase one = %v", reachMajority)

		// [Phase 2]
		if reachMajority {
			log.Info().Msgf("[RunPaxos] Reach promiseMajority -> Progress to phase 2")
			reachMajority = n.EnterPhaseTwo(decidedValue)
		} else {
			log.Info().Msgf("[RunPaxos] Haven't Reach promiseMajority -> Retry phase 1")
		}
	}

	// when the decided value == to the one you propose -> end the tag

	//
	decidedValue = *n.paxosCurrentState.acceptedValue
	log.Info().Msgf("[RunPaxos] after running Paxos decidedValue = %v", decidedValue)

	return decidedValue, nil
}
