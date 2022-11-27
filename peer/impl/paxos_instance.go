package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/types"
	"sync"
)

type safePaxosInstance struct {
	*sync.Mutex
	maxID               uint
	offsetID            uint
	currentLogicalClock uint
	maxIDProposedValue  *types.PaxosValue
	acceptedID          uint
	acceptedValue       *types.PaxosValue
	promises            []*types.PaxosPromiseMessage // store the received promise
}

func (pi *safePaxosInstance) UpdatePaxosOffsetID() {
	pi.Lock()
	defer pi.Unlock()

	pi.offsetID += 1
}

func (pi *safePaxosInstance) UpdatePaxosPromises(message *types.PaxosPromiseMessage) {
	pi.Lock()
	defer pi.Unlock()

	pi.promises = append(pi.promises, message)
}

func (pi *safePaxosInstance) FindAcceptedValueInPaxosPromises(proposedValue types.PaxosValue) types.PaxosValue {
	pi.Lock()
	defer pi.Unlock()

	numberOfAcceptedValue := 0

	// Find the correct AcceptedID and AcceptedValue by checking all promises
	maxAcceptedID := uint(0)
	selectedIndex := 0
	for i, promise := range pi.promises {
		if promise.AcceptedValue == nil {
			numberOfAcceptedValue += 1
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

func (pi *safePaxosInstance) UpdatePaxosAcceptedIDAndAcceptedValue(message *types.PaxosProposeMessage) {
	pi.Lock()
	defer pi.Unlock()

	pi.acceptedID = message.ID
	pi.acceptedValue = &message.Value
}
