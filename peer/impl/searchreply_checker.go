package impl

import "sync"

type searchReplyChecker struct {
	*sync.Mutex
	realSearchReplyChecker map[string]chan []string
}

func (src *searchReplyChecker) InitSearchReplyChecker(key string) {
	src.Lock()
	defer src.Unlock()

	src.realSearchReplyChecker[key] = make(chan []string)
}

func (src *searchReplyChecker) UpdateSearchReplyEntry(key string, data []string) {
	src.Lock()
	defer src.Unlock()

	// Question: Is the functionality correct?
	src.realSearchReplyChecker[key] <- data
}

func (src *searchReplyChecker) FindSearchReplyEntry(key string) chan []string {
	src.Lock()
	defer src.Unlock()

	return src.realSearchReplyChecker[key]
}

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
