package impl

import (
	"go.dedis.ch/cs438/peer"
	"math/rand"
	"sync"
	"time"
)

/* For Routable */

type safeRoutable struct {
	*sync.Mutex
	realTable peer.RoutingTable
}

func (t *safeRoutable) UpdateRoutingtable(key string, val string) {
	t.Lock()
	defer t.Unlock()

	t.realTable[key] = val
}

func (t *safeRoutable) DeleteRoutingEntry(key string) {
	t.Lock()
	defer t.Unlock()

	delete(t.realTable, key)
}

func (t *safeRoutable) FindRoutingEntry(key string) (string, bool) {
	t.Lock()
	defer t.Unlock()
	val, ok := t.realTable[key]
	return val, ok
}

func (t *safeRoutable) FindRandomNeighbor(origin string) []string {
	// Neighbor -> in the node table, find key == value
	// Only find one neighbor

	t.Lock()
	defer t.Unlock()
	rand.Seed(time.Now().Unix())
	var neighbor []string

	for key, val := range t.realTable {
		if key == val && origin != key {
			neighbor = append(neighbor, val)
		}
	}
	return neighbor

	//if len(neighbor) != 0 {
	//	n := rand.Int() % (len(neighbor))
	//	log.Info().Msgf("neighbor %v, Choosen neighbor index = %v", neighbor, n)
	//	return neighbor[n]
	//}
	//return ""

}
