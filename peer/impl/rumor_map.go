package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/types"
	"sync"
)

type safeRumorMap struct {
	*sync.Mutex
	realRumorMap map[string]types.RumorsMessage
}

func (m *safeRumorMap) UpdateRumorMap(remoteAddr string, rumor types.RumorsMessage) {
	m.Lock()
	defer m.Unlock()

	// TODO: need to add rumors in order
	m.realRumorMap[remoteAddr] = rumor
}

func (m *safeRumorMap) ReturnMissingRumorMap(remoteAddr string, lastSeq uint) []types.Rumor {
	m.Lock()
	defer m.Unlock()

	log.Info().Msgf("[ReturnMissingRumorMap] %v starting from seq %v ", m.realRumorMap[remoteAddr].Rumors, lastSeq)

	// TODO: Return the rumors after lastSeq
	var missingMsgRumor []types.Rumor

	for i, rumor := range m.realRumorMap[remoteAddr].Rumors {
		log.Info().Msgf("[ReturnMissingRumorMap] i = %v, lastSeq = %v", i, lastSeq)
		if i >= int(lastSeq) {
			log.Info().Msgf("[ReturnMissingRumorMap] Missing Rumor seq[%v] [%v]", rumor.Sequence, rumor.Msg)
			missingMsgRumor = append(missingMsgRumor, rumor)
		}
	}
	return missingMsgRumor
}
