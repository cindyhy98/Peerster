package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"strconv"
	"sync"
)

type safePaxosInstance struct {
	*sync.Mutex
	maxID               uint
	currentLogicalClock uint
	phase               int
	promises            []*types.PaxosPromiseMessage
	acceptedID          uint
	acceptedValue       *types.PaxosValue
	proposedValue       *types.PaxosValue
}

func (p *safePaxosInstance) GetPromises() []*types.PaxosPromiseMessage {
	p.Lock()
	defer p.Unlock()

	return p.promises
}

func (p *safePaxosInstance) AddPromises(peerPromise *types.PaxosPromiseMessage) {
	p.Lock()
	defer p.Unlock()

	p.promises = append(p.promises, peerPromise)
}

func (p *safePaxosInstance) FindAcceptedValueInPromises() *types.PaxosValue {
	p.Lock()
	defer p.Unlock()
	// Suppose that the promise is not nil
	numberOfAcceptedValue := 0

	// Find the correct AcceptedID and AcceptedValue by checking all p.promises
	maxAcceptedID := uint(0)
	selectedIndex := 0
	for i, promise := range p.promises {
		if promise.AcceptedValue == nil {
			numberOfAcceptedValue += 1
			continue
		}
		if promise.AcceptedID > maxAcceptedID {
			maxAcceptedID = promise.AcceptedID
			selectedIndex = i
		}
	}

	if numberOfAcceptedValue == len(p.promises) {
		// Question: is it correct?
		return p.proposedValue
	}

	//p.acceptedID = p.promises[selectedIndex].AcceptedID
	//p.acceptedValue = p.promises[selectedIndex].AcceptedValue

	return p.promises[selectedIndex].AcceptedValue
}

func (n *node) CheckPaxosPrepareMessage(msgPaxosPrepare *types.PaxosPrepareMessage) bool {
	shouldIgnore := false
	// Ignore messages whose Step field does not match your current logical clock
	if msgPaxosPrepare.Step != n.paxosInstance.currentLogicalClock {
		shouldIgnore = true
		log.Info().Msgf("Ignore old paxos prepare message %v %v", n.paxosInstance.currentLogicalClock, msgPaxosPrepare.Step)

	}

	if msgPaxosPrepare.ID <= n.paxosInstance.maxID {
		shouldIgnore = true
		log.Info().Msgf("Ignore a propose message as Paxos ID (%v) isn't equal the maxID (%v)", msgPaxosPrepare.ID, n.paxosInstance.maxID)
	} else {
		log.Info().Msgf("Update maxID to %v in prepare phase", msgPaxosPrepare.ID)
		n.paxosInstance.maxID = msgPaxosPrepare.ID
	}

	return shouldIgnore
}

func (n *node) CheckPaxosPromiseMessage(msgPaxosPromise *types.PaxosPromiseMessage) bool {
	shouldIgnore := false

	// Ignore messages whose Step field does not match your current logical clock
	if msgPaxosPromise.Step != n.paxosInstance.currentLogicalClock {
		shouldIgnore = true
		log.Info().Msgf("Ignore old paxos promise message %v %v", n.paxosInstance.currentLogicalClock, msgPaxosPromise.Step)

	}

	// Ignore messages if the proposer is not in Paxos phase 1
	if n.paxosInstance.phase != 1 {
		shouldIgnore = true
		log.Info().Msgf("Ignore a promise message as Paxos is in Phase [%v]", n.paxosInstance.phase)
	}

	return shouldIgnore
}

func (n *node) CheckPaxosProposeMessage(msgPaxosPropose *types.PaxosProposeMessage) bool {
	shouldIgnore := false
	// Ignore messages whose Step field does not match your current logical clock
	if msgPaxosPropose.Step != n.paxosInstance.currentLogicalClock {
		shouldIgnore = true
		log.Info().Msgf("Ignore old paxos propose message %v %v", n.paxosInstance.currentLogicalClock, msgPaxosPropose.Step)

	}

	if msgPaxosPropose.ID != n.paxosInstance.maxID {
		shouldIgnore = true
		log.Info().Msgf("Ignore a propose message as Paxos ID (%v) isn't equal the maxID (%v)", msgPaxosPropose.ID, n.paxosInstance.maxID)
	}

	return shouldIgnore
}

func (n *node) CheckPaxosAcceptMessage(msgPaxosAccept *types.PaxosAcceptMessage) bool {
	shouldIgnore := false

	// Ignore messages whose Step field does not match your current logical clock
	if msgPaxosAccept.Step != n.paxosInstance.currentLogicalClock {
		shouldIgnore = true
		log.Info().Msgf("Ignore old paxos accept message %v %v", n.paxosInstance.currentLogicalClock, msgPaxosAccept.Step)

	}

	// Ignore messages if the proposer is not in Paxos phase 1
	if n.paxosInstance.phase != 2 {
		shouldIgnore = true
		log.Info().Msgf("Ignore a accept message as Paxos is in Phase [%v]", n.paxosInstance.phase)
	}

	return shouldIgnore
}

func (n *node) RunPaxosProposer(proposedValue types.PaxosValue) (types.PaxosValue, error) {
	decidedValue := proposedValue

	n.BroadcastPrepare()

	prepareKey := n.ComputePaxosChannelKey("prepare", n.conf.PaxosID)

	errPrepare := n.Prepare(prepareKey)
	if errPrepare != nil {
		return decidedValue, errPrepare
	}

	promiseKey := n.ComputePaxosChannelKey("promise", n.conf.PaxosID)
	// Question: correct?

	errPromise := n.Promise(promiseKey)
	if errPromise != nil {
		return decidedValue, errPromise
	}

	proposeKey := n.ComputePaxosChannelKey("propose", n.conf.PaxosID)

	errPropose := n.Propose(proposeKey)
	if errPropose != nil {
		return decidedValue, errPropose
	}

	acceptKey := n.ComputePaxosChannelKey("accept", n.conf.PaxosID)

	errAccept := n.Accept(acceptKey)
	if errAccept != nil {
		return decidedValue, errAccept
	}

	// wait for enough promise value like hw2

	// wait for enough accepted value

	return decidedValue, nil
}

func (n *node) ComputePaxosChannelKey(msgType string, id uint) string {
	return n.conf.Socket.GetAddress() + "-" + msgType + "-" + strconv.Itoa(int(id))
}

func (n *node) BroadcastPrepare() {

	// Broadcast a PaxosPrepareMessage
	go func() {
		newPaxosPrepareMessage := types.PaxosPrepareMessage{
			Step:   uint(0),
			ID:     n.conf.PaxosID,
			Source: n.conf.Socket.GetAddress(),
		}

		transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newPaxosPrepareMessage)
		log.Info().Msgf("[BroadcastPrepare] [%v] Paxos Prepare => everyone", n.conf.Socket.GetAddress())

		_ = n.Broadcast(transMsg)
	}()
}

func (n *node) Prepare(key string) error {

	channel := n.paxosMsgChannel.InitPaxosMsgChannel(key)
	defer n.paxosMsgChannel.DeletePaxosMsgChannel(key)

	select {
	case msg := <-channel:
		msgPaxosPrepare, ok := msg.(*types.PaxosPrepareMessage)
		if !ok {
			return xerrors.Errorf("wrong type: %T", msg)
		}

		log.Info().Msgf("[Prepare] received msgPaxosPrepare %v", msgPaxosPrepare)
		// [Task1]
		if n.CheckPaxosPrepareMessage(msgPaxosPrepare) {
			// Ignore it
		} else {
			// Update paxos phase to 1
			n.paxosInstance.phase = 1

			// Response with a PaxosPromiseMessage inside PrivateMessage
			// In prepared step -> don't change the AcceptedID
			newPaxosPromiseMessage := types.PaxosPromiseMessage{
				Step:          msgPaxosPrepare.Step, // current TLC identifier
				ID:            msgPaxosPrepare.ID,   // proposer ID
				AcceptedID:    n.paxosInstance.acceptedID,
				AcceptedValue: n.paxosInstance.acceptedValue,
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

	}

	return nil
}

func (n *node) Promise(key string) error {
	channel := n.paxosMsgChannel.InitPaxosMsgChannel(key)
	defer n.paxosMsgChannel.DeletePaxosMsgChannel(key)

	select {
	case msg := <-channel:

		msgPaxosPromise, ok := msg.(*types.PaxosPromiseMessage)
		if !ok {
			return xerrors.Errorf("wrong type: %T", msg)
		}

		log.Info().Msgf("[Promise] received msgPaxosPromise %v", msgPaxosPromise)
		if n.CheckPaxosPromiseMessage(msgPaxosPromise) {
			// Ignore it
		} else {

			// TODO: wait until the threshold
			// Collect the PaxosPromiseMessage's until a threshold of peers replies
			if len(n.paxosInstance.GetPromises()) < n.conf.PaxosThreshold(n.conf.TotalPeers) {
				if msgPaxosPromise == nil {
					log.Info().Msgf("Received NIL paxos promise message")
				}

				n.paxosInstance.AddPromises(msgPaxosPromise)
			}

			log.Info().Msgf("Paxos Promise Threshold = %v", n.conf.PaxosThreshold(n.conf.TotalPeers))
			if len(n.paxosInstance.GetPromises()) == n.conf.PaxosThreshold(n.conf.TotalPeers) {
				// Update paxos phase to 2
				n.paxosInstance.phase = 2

				// Broadcast a PaxosProposeMessage
				newPaxosProposeMessage := types.PaxosProposeMessage{
					Step:  n.paxosInstance.currentLogicalClock,
					ID:    n.paxosInstance.maxID,
					Value: *n.paxosInstance.FindAcceptedValueInPromises(),
				}

				transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newPaxosProposeMessage)
				err := n.Broadcast(transMsg)
				return err

			}
		}

	}

	return nil
}

func (n *node) Propose(key string) error {
	channel := n.paxosMsgChannel.InitPaxosMsgChannel(key)
	defer n.paxosMsgChannel.DeletePaxosMsgChannel(key)

	select {
	case msg := <-channel:
		msgPaxosPropose, ok := msg.(*types.PaxosProposeMessage)
		if !ok {
			return xerrors.Errorf("wrong type: %T", msg)
		}
		if n.CheckPaxosProposeMessage(msgPaxosPropose) {
			// Ignore
		} else {
			// Question: should I change it here?
			//n.paxosInstance.acceptedID = msgPaxosPropose.ID
			//n.paxosInstance.acceptedValue = &msgPaxosPropose.Value

			newPaxosAcceptMessage := types.PaxosAcceptMessage{
				Step:  msgPaxosPropose.Step,
				ID:    msgPaxosPropose.ID,
				Value: msgPaxosPropose.Value,
			}
			transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newPaxosAcceptMessage)
			err := n.Broadcast(transMsg)
			return err
		}

	}
	return nil
}

func (n *node) Accept(key string) error {
	channel := n.paxosMsgChannel.InitPaxosMsgChannel(key)
	defer n.paxosMsgChannel.DeletePaxosMsgChannel(key)

	select {
	case msg := <-channel:
		msgPaxosAccept, ok := msg.(*types.PaxosAcceptMessage)
		if !ok {
			return xerrors.Errorf("wrong type: %T", msg)
		}

		log.Info().Msgf("[Accept] %v", msgPaxosAccept)
	}

	return nil
}
