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
		//log.Info().Msgf("[EditStatus] Expected Rumor")
		s.realLastStatus[key] = seq
		return seq, true
	case seq == 1 && !ok:
		// first rumor (should be in the expected rumor case)
		s.realLastStatus[key] = 1
		return 1, true
	case val != seq-1 && ok:
		// non expected rumor
		return val, false
	default:
		return 0, false
	}

}

func (s *safeStatus) Freeze() types.StatusMessage {
	copyOfMap := types.StatusMessage{}

	s.Lock()
	defer s.Unlock()
	for k, v := range s.realLastStatus {
		copyOfMap[k] = v
	}
	return copyOfMap
}

/*
func (s *safeStatus) SyncRemoteStatus(r *safeStatus) (int, string, uint) {
	// TODO: should I add another mutex for the remote StatusMessage?

	s.Lock()
	r.Lock()
	defer r.Unlock()
	defer s.Unlock()
	var lastSeq uint
	var key string
	localStatus := s.realLastStatus
	remoteStatus := r.realLastStatus

	res := 0
	// TODO: should return different cases
	for rk := range remoteStatus {
		remoteSeq := remoteStatus[rk]

		// [Case 1]: Remote peer has new message -> local peer need to send a status message to remote
		// -> 1. Key only exists in remoteStatus, not in localStatus
		// -> 2. If Key exists in both cases, check if remoteSeq is higher than localSeq
		if _, ok := localStatus[rk]; !ok {
			log.Info().Msgf("[SyncRemoteStatus] [Case1]: message [%v]:[%v] only in remote peer", rk, remoteSeq)
			res = 1
		}
	}
	for lk := range localStatus {
		localSeq := localStatus[lk]
		// [Case 2]: Local peer has new message -> local peer need to send all missing rumors to remote
		// -> 1. Key only exists in localStatus, not in remoteStatus
		// -> 2. If Key exists in both cases, check if localSeq is higher than remoteSeq
		remoteSeq, ok := remoteStatus[lk]

		if !ok || localSeq > remoteSeq {
			lastSeq = localStatus[lk]
			key = lk
			if res == 0 {
				// Remote peer doesn't has new message
				log.Info().Msgf("[SyncRemoteStatus] [Case2]: Local peer has new message [%v]:[%v]", lk, localSeq)
				res = 2
			} else if res == 1 {
				// Remote peer also has new message, yet only local peer need to send back all missing rumors
				log.Info().Msgf("[SyncRemoteStatus] [Case3]: Both peers have new messages [%v]:[%v]", lk, localSeq)
				res = 3
			}
		}

		//if !ok && res == 0 {
		//
		//	log.Info().Msgf("[SyncRemoteStatus] [Case2]: Local peer has new message [%v]:[%v]", lk, localStatus[lk])
		//	lastSeq = localStatus[lk]
		//	res = 2
		//} else if res == 1 {
		//	log.Info().Msgf("[SyncRemoteStatus] [Case3]: Both peers have new messages [%v]", localStatus[lk])
		//	res = 3
		//}
	}

	if res == 0 {
		log.Info().Msgf("[SyncRemoteStatus] [Case4]: Same View")
	}

	return res, key, lastSeq
}
*/
