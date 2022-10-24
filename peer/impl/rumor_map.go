package impl

import (
	"go.dedis.ch/cs438/types"
	"sync"
)

type safeRumorMap struct {
	*sync.Mutex
	realRumorMap map[string][]types.Rumor
}

func (m *safeRumorMap) UpdateRumorMap(remoteAddr string, rumor types.Rumor) {
	m.Lock()
	defer m.Unlock()

	m.realRumorMap[remoteAddr] = append(m.realRumorMap[remoteAddr], rumor)
	//log.Info().Msgf("[UpdateRumorMap] add %v to rumor map %v:%v", rumor, remoteAddr, m.realRumorMap[remoteAddr])
	//m.realRumorMap[remoteAddr] = rumor
}

func (m *safeRumorMap) ReturnMissingRumorMap(remoteAddr string, lastSeq uint) []types.Rumor {
	m.Lock()
	defer m.Unlock()

	//log.Info().Msgf("[ReturnMissingRumorMap] %v starting from seq %v ", m.realRumorMap[remoteAddr], lastSeq)

	// Return the rumors after lastSeq
	var missingMsgRumor []types.Rumor

	for i, rumor := range m.realRumorMap[remoteAddr] {
		//log.Info().Msgf("[ReturnMissingRumorMap] i = %v, lastSeq = %v", i, lastSeq)
		if i >= int(lastSeq) {

			missingMsgRumor = append(missingMsgRumor, rumor)

		}
	}

	//
	return missingMsgRumor
}
