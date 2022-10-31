package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"math/rand"
	"sync"
	"time"
)

/* For Routingtable */

type safeRoutingtable struct {
	*sync.Mutex
	realTable peer.RoutingTable
}

func (t *safeRoutingtable) UpdateRoutingtable(key string, val string) {
	t.Lock()
	defer t.Unlock()

	t.realTable[key] = val
}

func (t *safeRoutingtable) DeleteRoutingEntry(key string) {
	t.Lock()
	defer t.Unlock()

	delete(t.realTable, key)
}

func (t *safeRoutingtable) FindRoutingEntry(key string) (string, bool) {
	t.Lock()
	defer t.Unlock()
	val, ok := t.realTable[key]
	return val, ok
}

func (t *safeRoutingtable) FindNeighbor(origin string) []string {
	// Neighbor -> in the node table, find key == value and don't include yourself

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
}

func (t *safeRoutingtable) FindNeighborWithoutContain(origin string, addr string) []string {
	// Neighbor -> in the node table, find key == value
	// Only find one neighbor

	t.Lock()
	defer t.Unlock()
	rand.Seed(time.Now().Unix())
	var neighbor []string

	for key, val := range t.realTable {
		if key == val && origin != key && addr != key {
			neighbor = append(neighbor, val)
		}
	}

	log.Info().Msgf("[FindNeighborWithoutContain] all neighbor = %v", neighbor)
	return neighbor
}
