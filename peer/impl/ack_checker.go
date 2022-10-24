package impl

import (
	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

type ackChecker struct {
	*sync.Mutex
	realAckChecker map[string]*time.Timer
}

func (ac *ackChecker) UpdateAckChecker(packetID string, ackTimeout *time.Timer) {
	ac.Lock()
	defer ac.Unlock()

	ac.realAckChecker[packetID] = ackTimeout

}

func (ac *ackChecker) FindAckEntryAndStopTimerIfEqual(packetID string) {
	ac.Lock()
	defer ac.Unlock()

	T, ok := ac.realAckChecker[packetID]
	if ok {
		//n.ackRecord.received = true
		T.Stop()
		log.Info().Msgf("[WaitForAck] Stop! ")
	}
}
