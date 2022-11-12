package impl

import (
	"go.dedis.ch/cs438/peer"
	"sync"
)

type safeCatalog struct {
	*sync.Mutex
	realCatalog peer.Catalog
}

func (c *safeCatalog) FindCatalogEntry(key string) []string {
	c.Lock()
	defer c.Unlock()
	peers, ok := c.realCatalog[key]
	if ok {
		var result []string
		for p := range peers {
			result = append(result, p)
		}
		return result
	} else {
		return nil
	}
}

func (c *safeCatalog) UpdateCatalogWithMutex(key string, peer string) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.realCatalog[key]; !ok {
		c.realCatalog[key] = make(map[string]struct{})
	}

	// Question: Wht does struct takes in two {}
	c.realCatalog[key][peer] = struct{}{}
}

func (c *safeCatalog) Freeze() peer.Catalog {
	copyOfMap := peer.Catalog{}

	c.Lock()
	defer c.Unlock()
	for k, v := range c.realCatalog {
		copyOfMap[k] = v
	}
	return copyOfMap
}
