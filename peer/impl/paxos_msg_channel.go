package impl

import (
	"go.dedis.ch/cs438/types"
	"sync"
)

type safePaxosMsgChannel struct {
	*sync.Mutex
	realMsgChannel map[string]chan types.Message
}

func (pmc *safePaxosMsgChannel) InitPaxosMsgChannel(key string) chan types.Message {
	pmc.Lock()
	defer pmc.Unlock()

	pmc.realMsgChannel[key] = make(chan types.Message)
	return pmc.realMsgChannel[key]
}

func (pmc *safePaxosMsgChannel) SendToPaxosHandler(key string, data types.Message) {
	pmc.Lock()
	pmc.Unlock()

	pmc.realMsgChannel[key] <- data
}

func (pmc *safePaxosMsgChannel) DeletePaxosMsgChannel(key string) {
	pmc.Lock()
	defer pmc.Unlock()

	channel, ok := pmc.realMsgChannel[key]
	if !ok {
		return
	}

	delete(pmc.realMsgChannel, key)

	// Drain item
	for len(channel) > 0 {
		<-channel
	}

	close(channel)
}
