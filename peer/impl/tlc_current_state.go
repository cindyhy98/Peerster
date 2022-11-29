package impl

import (
	"go.dedis.ch/cs438/types"
	"sync"
)

type SafeTlcCurrentState struct {
	*sync.Mutex
	currentLogicalClock uint
	hasBroadcast        bool
	futureTLCMessage    map[uint]types.TLCMessage
}

func (tlc *SafeTlcCurrentState) IncrementStep() {
	tlc.Lock()
	defer tlc.Unlock()

	tlc.currentLogicalClock++
}

func (tlc *SafeTlcCurrentState) UpdateHasBroadcast() {
	tlc.Lock()
	defer tlc.Unlock()

	tlc.hasBroadcast = true
}

func (tlc *SafeTlcCurrentState) UpdateFutureTLCMessage(step uint, message types.TLCMessage) {
	tlc.Lock()
	defer tlc.Unlock()

	tlc.futureTLCMessage[step] = message
}

func (tlc *SafeTlcCurrentState) GetFutureTLCMessage(step uint) (types.TLCMessage, bool) {
	tlc.Lock()
	defer tlc.Unlock()

	val, ok := tlc.futureTLCMessage[step]
	return val, ok
}
