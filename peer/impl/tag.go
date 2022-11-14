package impl

import (
	"sync"
)

type safeTag struct {
	*sync.Mutex
	realTag map[string]string
}

func (t *safeTag) UpdateTagRecord(name string, metahash string) {
	t.Lock()
	defer t.Unlock()

	t.realTag[name] = metahash
}

func (t *safeTag) Freeze() map[string]string {
	copyOfMap := make(map[string]string)

	t.Lock()
	defer t.Unlock()
	for k, v := range t.realTag {
		copyOfMap[k] = v
	}

	return copyOfMap
}
