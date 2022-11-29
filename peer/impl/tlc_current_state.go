package impl

import (
	"go.dedis.ch/cs438/types"
	"sync"
)

type safeTlcCurrentState struct {
	*sync.Mutex
	currentLogicalClock uint
	hasBroadcast        bool
	futureTLCMessage    map[uint]types.TLCMessage
}

func (tlc *safeTlcCurrentState) IncrementStep() {
	tlc.Lock()
	defer tlc.Unlock()

	tlc.currentLogicalClock += 1
}

func (tlc *safeTlcCurrentState) UpdateHasBroadcast() {
	tlc.Lock()
	defer tlc.Unlock()

	tlc.hasBroadcast = true
}

func (tlc *safeTlcCurrentState) UpdateFutureTLCMessage(step uint, message types.TLCMessage) {
	tlc.Lock()
	defer tlc.Unlock()

	tlc.futureTLCMessage[step] = message
}

func (tlc *safeTlcCurrentState) GetFutureTLCMessage(step uint) (types.TLCMessage, bool) {
	tlc.Lock()
	defer tlc.Unlock()

	val, ok := tlc.futureTLCMessage[step]
	return val, ok
}
