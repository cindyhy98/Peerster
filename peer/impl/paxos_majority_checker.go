package impl

import (
	"sync"
)

type safePaxosMajorityChecker struct {
	*sync.Mutex
	counter  map[uint]map[string]int //[step][UniqueID]counter
	notifier map[uint]chan bool
}

/* Counter Functionality */

func (pmc *safePaxosMajorityChecker) UpdateAndGetCounter(key uint, UniqueID string) int {
	pmc.Lock()
	defer pmc.Unlock()

	// For PaxosPropose message
	if UniqueID == "" {
		UniqueID = "-"
	}

	if _, ok := pmc.counter[key]; !ok {
		pmc.counter[key] = make(map[string]int)
	}

	if _, ok := pmc.counter[key][UniqueID]; !ok {
		pmc.counter[key][UniqueID] = 0
	}

	// Increase the counter
	pmc.counter[key][UniqueID]++
	return pmc.counter[key][UniqueID]

}

//func (pmc *safePaxosMajorityChecker) GetCounter(key uint, UniqueID string) int {
//	pmc.Lock()
//	defer pmc.Unlock()
//
//	return pmc.counter[key][UniqueID]
//
//}

/* Notifier Functionality */

func (pmc *safePaxosMajorityChecker) InitNotifier(key uint) chan bool {
	pmc.Lock()
	defer pmc.Unlock()

	pmc.notifier[key] = make(chan bool)
	return pmc.notifier[key]
}

func (pmc *safePaxosMajorityChecker) GetNotifier(key uint) chan bool {
	pmc.Lock()
	defer pmc.Unlock()

	return pmc.notifier[key]
}

func (pmc *safePaxosMajorityChecker) UpdateNotifier(key uint, reachMajority bool) {
	pmc.Lock()
	defer pmc.Unlock()

	pmc.notifier[key] <- reachMajority
}

func (pmc *safePaxosMajorityChecker) DeleteNotifier(key uint) {
	pmc.Lock()
	defer pmc.Unlock()

	channel, ok := pmc.notifier[key]
	if !ok {
		return
	}

	delete(pmc.notifier, key)

	// Drain item
	for len(channel) > 0 {
		<-channel
	}

	close(channel)
}
