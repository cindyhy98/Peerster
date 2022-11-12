package impl

import (
	"sync"
)

type replyChecker struct {
	*sync.Mutex
	realReplyChecker map[string]chan []byte
}

func (rc *replyChecker) OpenReplyChecker(key string) {
	rc.Lock()
	defer rc.Unlock()

	// Question: Is the functionality correct?
	rc.realReplyChecker[key] = make(chan []byte)
}

func (rc *replyChecker) UpdateReplyEntry(key string, data []byte) {
	rc.Lock()
	defer rc.Unlock()

	// Question: Is the functionality correct?
	rc.realReplyChecker[key] <- data
}

func (rc *replyChecker) FindReplyEntry(key string) chan []byte {
	rc.Lock()
	defer rc.Unlock()

	return rc.realReplyChecker[key]
}

// Not sure if we need this or not
/*
func (rc *replyChecker) Freeze() map[string]chan []byte {
	copyOfMap := map[string]chan []byte

	rc.Lock()
	defer rc.Unlock()
	for k, v := range rc.realReplyChecker {
		copyOfMap[k] = v
	}
	return copyOfMap
}


*/
