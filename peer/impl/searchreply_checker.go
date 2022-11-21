package impl

import (
	"go.dedis.ch/cs438/types"
	"sync"
)

type searchReplyChecker struct {
	*sync.Mutex
	realSearchReplyChecker map[string]chan []types.FileInfo
}

func (src *searchReplyChecker) InitSearchReplyChecker(key string) chan []types.FileInfo {
	src.Lock()
	defer src.Unlock()

	src.realSearchReplyChecker[key] = make(chan []types.FileInfo)
	return src.realSearchReplyChecker[key]
}

func (src *searchReplyChecker) UpdateSearchReplyEntry(key string, data []types.FileInfo) {
	src.Lock()
	defer src.Unlock()

	src.realSearchReplyChecker[key] <- data
}

//func (src *searchReplyChecker) FindSearchReplyEntry(key string) chan []types.FileInfo {
//	src.Lock()
//	defer src.Unlock()
//
//	return src.realSearchReplyChecker[key]
//}

func (src *searchReplyChecker) DeleteSearchReplyChecker(key string) {
	src.Lock()
	defer src.Unlock()

	channel, ok := src.realSearchReplyChecker[key]
	if !ok {
		return
	}

	delete(src.realSearchReplyChecker, key)

	// Drain item
	for len(channel) > 0 {
		<-channel
	}

	close(channel)

}
