package impl

import (
	"sync"
)

type dataReplyChecker struct {
	*sync.Mutex
	realDataReplyChecker map[string]chan []byte
}

func (drc *dataReplyChecker) InitDataReplyChecker(key string) {
	drc.Lock()
	defer drc.Unlock()

	drc.realDataReplyChecker[key] = make(chan []byte)
}

func (drc *dataReplyChecker) UpdateDataReplyEntry(key string, data []byte) {
	drc.Lock()
	defer drc.Unlock()

	// Question: Is the functionality correct?
	drc.realDataReplyChecker[key] <- data
}

func (drc *dataReplyChecker) FindDataReplyEntry(key string) chan []byte {
	drc.Lock()
	defer drc.Unlock()

	return drc.realDataReplyChecker[key]
}

// Not sure if we need this or not
/*
func (drc *dataReplyChecker) Freeze() map[string]chan []byte {
	copyOfMap := map[string]chan []byte

	drc.Lock()
	defer drc.Unlock()
	for k, v := range drc.realDataReplyChecker {
		copyOfMap[k] = v
	}
	return copyOfMap
}


*/
