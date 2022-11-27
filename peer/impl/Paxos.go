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
			Step:   uint(0),
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
	notifierChannel := n.paxosPromiseMajority.GetNotifier(n.paxosInstance.currentLogicalClock)
	defer n.paxosPromiseMajority.DeleteNotifier(n.paxosInstance.currentLogicalClock)

	for hasTimeout := false; !hasTimeout; {
		select {
		case notifier = <-notifierChannel:
			// Reach a majority of PaxosPromises -> progress to phase two
			log.Info().Msgf("[WaitForPaxosPromise] get a majority of promises")

		case <-time.After(timeout):
			log.Info().Msgf("[WaitForPaxosPromise] timeout reaches")
			hasTimeout = true
		}
	}

	return notifier
}

func (n *node) EnterPhaseOne() bool {
	reachPromiseMajority := false

	Id := n.conf.PaxosID + n.paxosInstance.offsetID*n.conf.TotalPeers
	n.BroadcastPaxosPrepare(Id)

	// Wait for a majority of PaxosPromise
	reachPromiseMajority = n.WaitForPaxosPromise(n.conf.PaxosProposerRetry)

	if !reachPromiseMajority {
		// For Retry after timeout
		n.paxosInstance.UpdatePaxosOffsetID()
	}

	return reachPromiseMajority

}

func (n *node) BroadcastPaxosPropose(proposedValue types.PaxosValue) {

	go func() {
		// Broadcast a PaxosProposeMessage
		newPaxosProposeMessage := types.PaxosProposeMessage{
			Step:  n.paxosInstance.currentLogicalClock,
			ID:    n.paxosInstance.maxID,
			Value: n.paxosInstance.FindAcceptedValueInPaxosPromises(proposedValue),
		}

		transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newPaxosProposeMessage)
		log.Info().Msgf("[BroadcastPaxosPropose] [%v] Paxos Propose => everyone", n.conf.Socket.GetAddress())

		_ = n.Broadcast(transMsg)
	}()

}

func (n *node) WaitForPaxosAccept(timeout time.Duration) bool {
	notifier := false
	notifierChannel := n.paxosAcceptMajority.GetNotifier(n.paxosInstance.currentLogicalClock)
	defer n.paxosAcceptMajority.DeleteNotifier(n.paxosInstance.currentLogicalClock)

	for hasTimeout := false; !hasTimeout; {
		select {
		case notifier = <-notifierChannel:
			// Reach a majority of PaxosPromises -> progress to phase two
			log.Info().Msgf("[WaitForPaxosAccept] get a majority of accept")

		case <-time.After(timeout):
			log.Info().Msgf("[WaitForPaxosAccept] timeout reaches")
			hasTimeout = true
		}
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
		n.paxosInstance.UpdatePaxosOffsetID()
	}

	return reachAcceptMajority

}

func (n *node) RunPaxos(proposedValue types.PaxosValue) (types.PaxosValue, error) {

	decidedValue := proposedValue
	log.Info().Msgf("[RunPaxos] Before running Paxos decidedValue = %v", decidedValue)

	reachMajority := false
	// [Phase 1]
	for !reachMajority {
		reachMajority = n.EnterPhaseOne()

		// [Phase 2]
		if reachMajority {
			log.Info().Msgf("[RunPaxos] Reach promiseMajority -> Progress to phase 2")
			reachMajority = n.EnterPhaseTwo(decidedValue)
		} else {
			log.Info().Msgf("[RunPaxos] Haven't Reach promiseMajority -> Retry phase 1")
		}
	}

	decidedValue = *n.paxosInstance.acceptedValue
	log.Info().Msgf("[RunPaxos] after running Paxos decidedValue = %v", decidedValue)

	return decidedValue, nil
}
