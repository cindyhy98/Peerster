package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"sync"
)

type safePaxosInstance struct {
	*sync.Mutex
	maxID         uint
	AcceptedID    uint
	AcceptedValue *types.PaxosValue
}

func (n *node) CheckPaxosPrepareMessage(msgPaxosPrepare *types.PaxosPrepareMessage) bool {
	shouldIgnore := false
	// If the step != 0 -> Ignore
	if msgPaxosPrepare.Step != 0 {
		shouldIgnore = true
		log.Info().Msgf("[CheckPaxosPrepareMessage] Ignore this message with step %v", msgPaxosPrepare.Step)

	}

	if msgPaxosPrepare.ID <= n.paxosInstance.maxID {
		shouldIgnore = true
		log.Info().Msgf("[CheckPaxosPrepareMessage] Ignore this message with wrong ID %v", msgPaxosPrepare.ID)
	} else {
		n.paxosInstance.maxID = msgPaxosPrepare.ID
	}

	return shouldIgnore
}

func (n *node) ExecPaxosPrepareMessage(msg types.Message, pkt transport.Packet) error {
	msgPaxosPrepare, ok := msg.(*types.PaxosPrepareMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msgf("[ExecPaxosPrepareMessage] %v ", msgPaxosPrepare)

	// [Task1]
	if n.CheckPaxosPrepareMessage(msgPaxosPrepare) {
		// Ignore it
	} else {
		// Response with a PaxosPromiseMessage inside PrivateMessage
		// In prepared step -> don't change the AcceptedID

		newPaxosPromiseMessage := types.PaxosPromiseMessage{
			Step:          msgPaxosPrepare.Step,
			ID:            msgPaxosPrepare.ID,
			AcceptedID:    n.paxosInstance.AcceptedID,
			AcceptedValue: n.paxosInstance.AcceptedValue,
		}

		transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newPaxosPromiseMessage)

		recipients := make(map[string]struct{}, 0)
		recipients[msgPaxosPrepare.Source] = struct{}{}
		newPrivateMessage := types.PrivateMessage{
			Recipients: recipients,
			Msg:        &transMsg,
		}
		transMsgBroadcast, _ := n.conf.MessageRegistry.MarshalMessage(newPrivateMessage)

		err := n.Broadcast(transMsgBroadcast)
		return err
	}

	return nil
}

func (n *node) ExecPaxosPromiseMessage(msg types.Message, pkt transport.Packet) error {
	msgPaxosPromise, ok := msg.(*types.PaxosPromiseMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msgf("[ExecPaxosPromiseMessage] %v ", msgPaxosPromise)

	return nil
}

func (n *node) ExecPaxosProposeMessage(msg types.Message, pkt transport.Packet) error {
	msgPaxosPropose, ok := msg.(*types.PaxosPromiseMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msgf("[ExecPaxosProposeMessage] %v ", msgPaxosPropose)
	return nil
}

func (n *node) ExecPaxosAcceptMessage(msg types.Message, pkt transport.Packet) error {
	msgPaxosAccept, ok := msg.(*types.PaxosAcceptMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msgf("[ExecPaxosAcceptMessage] %v ", msgPaxosAccept)

	return nil
}
