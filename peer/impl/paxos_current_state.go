package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/types"
	"sync"
)

type SafePaxosCurrentState struct {
	*sync.Mutex
	maxID            uint
	offsetID         uint
	acceptedID       uint                         // acceptor need to store
	acceptedValue    *types.PaxosValue            // acceptor need to store
	finalAcceptValue *types.PaxosValue            // proposer need to store
	promises         []*types.PaxosPromiseMessage // store the received promise
}

func (pi *SafePaxosCurrentState) UpdatePaxosOffsetID() {
	pi.Lock()
	defer pi.Unlock()

	pi.offsetID++
}

func (pi *SafePaxosCurrentState) StorePaxosPromises(message *types.PaxosPromiseMessage) {
	pi.Lock()
	defer pi.Unlock()

	pi.promises = append(pi.promises, message)
}

func (pi *SafePaxosCurrentState) FindAcceptedValueInPaxosPromises(proposedValue types.PaxosValue) types.PaxosValue {
	pi.Lock()
	defer pi.Unlock()

	numberOfAcceptedValue := 0

	// Find the correct AcceptedID and AcceptedValue by checking all promises
	maxAcceptedID := uint(0)
	selectedIndex := 0
	for i, promise := range pi.promises {
		if promise.AcceptedValue == nil {
			numberOfAcceptedValue++
			continue
		}
		if promise.AcceptedID > maxAcceptedID {
			maxAcceptedID = promise.AcceptedID
			selectedIndex = i
		}
	}

	if numberOfAcceptedValue == len(pi.promises) {
		log.Info().Msgf("[FindAcceptedValueInPromises] no PaxosPromiseMessage contained an AcceptedValue")
		return proposedValue
	}

	return *pi.promises[selectedIndex].AcceptedValue
}

func (pi *SafePaxosCurrentState) UpdatePaxosAcceptedIDAndAcceptedValue(message types.PaxosProposeMessage) {
	pi.Lock()
	defer pi.Unlock()

	pi.acceptedID = message.ID
	pi.acceptedValue = &message.Value
}

func (pi *SafePaxosCurrentState) UpdateFinalAcceptValue(message types.PaxosAcceptMessage) {
	pi.Lock()
	defer pi.Unlock()

	pi.finalAcceptValue = &message.Value
}
