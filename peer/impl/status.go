package impl

import (
	"go.dedis.ch/cs438/types"
	"sync"
)

/* For Status */

type safeStatus struct {
	*sync.Mutex
	realLastStatus types.StatusMessage
}

func (s *safeStatus) UpdateStatus(key string, val uint) {
	s.Lock()
	defer s.Unlock()

	s.realLastStatus[key] = val
}

func (s *safeStatus) FindStatusEntry(key string) (uint, bool) {
	s.Lock()
	defer s.Unlock()
	val, ok := s.realLastStatus[key]
	return val, ok
}

func (s *safeStatus) GetandIncrementStatusIfEqual(key string, seq uint) (uint, bool) {
	s.Lock()
	defer s.Unlock()

	val, ok := s.realLastStatus[key]

	switch {
	case val == seq-1 && ok:
		// expected rumor
		s.realLastStatus[key] = seq
		return seq, true
	case val == 0 && ok:
		// first rumor (should be in the expected rumor case)
		s.realLastStatus[key] = 1
		return 1, true
	case val != seq-1 && ok:
		// non expected rumor
		return val, false
	case !ok:
		return 0, false
	default:
		return 0, false
	}

}
