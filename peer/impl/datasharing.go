package impl

import (
	"bytes"
	"crypto"
	"encoding/hex"
	"errors"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/types"
	"io"
	"math/rand"
	"regexp"
	"time"
)

/* Supplement functions for datasharing */

func (n *node) SHA(chunk []byte) ([]byte, string) {
	// Calculate the hex-encoded chunk’s hash
	h := crypto.SHA256.New()
	h.Write(chunk)
	chuckHashSlice := h.Sum(nil)
	chunkHashHex := hex.EncodeToString(chuckHashSlice)
	return chuckHashSlice, chunkHashHex
}

func (n *node) SendDataRequest(metahash string, chosenPeer string, timeout time.Duration,
	attempt uint) ([]byte, error) {
	retryMax := n.conf.BackoffDataRequest.Retry
	factor := n.conf.BackoffDataRequest.Factor
	// send request to a chosenPeer
	requestID := xid.New().String()
	newDataReqMsg := types.DataRequestMessage{
		RequestID: requestID,
		Key:       metahash,
	}

	transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newDataReqMsg)
	log.Info().Msgf("[SendDataRequest] [%v] DataRequest =>  [%v]", n.conf.Socket.GetAddress(), chosenPeer)
	err := n.Unicast(chosenPeer, transMsg)

	if err != nil {
		return nil, err
	}
	n.dataReply.InitDataReplyChecker(requestID)

	select {
	case data := <-n.dataReply.FindDataReplyEntry(requestID):
		log.Info().Msgf("[SendDataRequest] find the reply entry")
		return data, nil
	case <-time.After(timeout):
		if attempt > retryMax {
			return nil, errors.New("[SendDataRequest] max attempts reached")
		}
		return n.SendDataRequest(metahash, chosenPeer, timeout*time.Duration(factor), attempt+1)
	}
}

func (n *node) FindMetafileFromPeer(metahash string, chosenPeer string) ([]byte, error) {
	// If the metafile doesn't exist in the node's local
	// -> ask catalog and see if it exists in the remote peer
	// -> store the chunk that is sent by the remote peer to the local storage
	// -> notify the local to return the stored chunks

	log.Info().Msgf("[FindChunkFromPeer] request for metafile with hash = %v", metahash)
	metafilePeer, err := n.SendDataRequest(metahash, chosenPeer, n.conf.BackoffDataRequest.Initial, uint(0))

	if err != nil {
		return []byte{}, err
	}

	return metafilePeer, nil
}

func (n *node) FindChunkFromPeer(metafile []byte, chosenPeer string) error {

	metafileTrim := bytes.Split(metafile, []byte(peer.MetafileSep))
	for _, chunkHashHex := range metafileTrim {
		log.Info().Msgf("[FindChunkFromPeer] request for chunk with hash = %v", string(chunkHashHex))
		if n.conf.Storage.GetDataBlobStore().Get(string(chunkHashHex)) == nil {

			_, err := n.SendDataRequest(string(chunkHashHex), chosenPeer, n.conf.BackoffDataRequest.Initial, uint(0))

			// Task 3: In case the remote peer responds with an empty value,
			// or the backoff timeouts, the function must return an error
			if err != nil {
				log.Error().Msgf("[FindChunkFromPeer Error] could find chunk %v", err)
				return err
			}
		}
	}

	return nil
}

func (n *node) FindChunkFromLocal(metafile []byte) ([]byte, error) {
	// If the metafile exists -> parse the metafile

	// Get the storage from n.conf
	nodeStorage := n.conf.Storage.GetDataBlobStore()
	chunksFile := make([]byte, 0)

	metafileTrim := bytes.Split(metafile, []byte(peer.MetafileSep))
	for _, chunkHashHex := range metafileTrim {
		chunk := nodeStorage.Get(string(chunkHashHex))

		chunksFile = append(chunksFile, chunk...)
	}
	return chunksFile, nil
}

func (n *node) SendSearchRequest(reg string, budget uint, chosenNeighbor string, requestID string,
	origin string) error {
	// send request to a chosenNeighbor

	newSearchReqMsg := types.SearchRequestMessage{
		RequestID: requestID,
		Origin:    origin,
		Pattern:   reg,
		Budget:    budget,
	}

	transMsg, _ := n.conf.MessageRegistry.MarshalMessage(newSearchReqMsg)
	err := n.Unicast(chosenNeighbor, transMsg)

	// Question: is this correct?
	return err

}

func (n *node) WaitForSearchReply(timeout time.Duration, channel chan []types.FileInfo) ([]types.FileInfo, error) {
	//wait for reply!!

	totalData := make([]types.FileInfo, 0)

	for hasTimeout := false; !hasTimeout; {
		select {
		case data := <-channel:
			log.Info().Msgf("[WaitForSearchReply] find the reply entry %v "+
				"but shouldn't deal with it before timeout", data)
			totalData = append(totalData, data...)
		case <-time.After(timeout):
			log.Info().Msgf("[WaitForSearchReply] timeout reaches")
			hasTimeout = true
		}
	}

	return totalData, nil

}

func (n *node) FindBudgetPerNeighbor(neighbors []string, budget uint) ([]string, uint) {

	if len(neighbors) != 0 {
		// Shuffle the neighbor
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(neighbors), func(i, j int) {
			neighbors[i], neighbors[j] = neighbors[j], neighbors[i]
		})

		budgetPerNeighbor := budget / uint(len(neighbors))
		return neighbors, budgetPerNeighbor

	}

	// no neighbor -> only return the node's matching names
	return []string{}, uint(0)

}

func (n *node) FindTagFromLocal(reg regexp.Regexp) ([]string, error) {
	// Update every tag from the neighbors to this tagCopy and then return these tag string in the end
	// Get the storage from n.conf
	names := make([]string, 0)

	n.conf.Storage.GetNamingStore().ForEach(func(name string, metahash []byte) bool {
		switch reg.MatchString(name) {
		case true:
			names = append(names, name)
		case false:
			// Do nothing
		}
		return true
	})

	return names, nil
}

func (n *node) SearchFirstFromNeighbors(pattern regexp.Regexp, budget uint, budgetPerNeighbor uint,
	shuffleNeighbors []string, timeout time.Duration) (string, error) {

	switch budgetPerNeighbor {
	case 0:
		for j := 0; j < int(budget); j++ {
			requestID := xid.New().String()
			channel := n.searchReply.InitSearchReplyChecker(requestID)
			log.Info().Msgf("[SearchFirst] [%v] searchReq => [%v]", n.conf.Socket.GetAddress(), shuffleNeighbors[j])
			_ = n.SendSearchRequest(pattern.String(), uint(1), shuffleNeighbors[j], requestID, n.conf.Socket.GetAddress())

			// Error handling?
			receivedData, _ := n.WaitForSearchReply(timeout, channel)
			n.searchReply.DeleteSearchReplyChecker(requestID)

			//receivedData := n.SearchFirstFromNeighbors(pattern, uint(1), shuffleNeighbors[j], conf.Timeout)

			foundNameFromPeer, isAllChunkAvaliable := n.CheckIfAllChunkAvailable(receivedData)

			if foundNameFromPeer != "" && isAllChunkAvaliable {
				return foundNameFromPeer, nil
			}
		}
	default:
		for j := 0; j < len(shuffleNeighbors); j++ {
			if j == len(shuffleNeighbors)-1 {
				// last one -> budget should be the rest
				usedBudget := uint(len(shuffleNeighbors)-1) * budgetPerNeighbor
				budgetPerNeighbor = budget - usedBudget
			}

			requestID := xid.New().String()
			channel := n.searchReply.InitSearchReplyChecker(requestID)
			log.Info().Msgf("[SearchFirst] [%v] searchReq => [%v]", n.conf.Socket.GetAddress(),
				shuffleNeighbors[j])
			_ = n.SendSearchRequest(pattern.String(), budgetPerNeighbor, shuffleNeighbors[j],
				requestID, n.conf.Socket.GetAddress())
			// Error handling?
			receivedData, _ := n.WaitForSearchReply(timeout, channel)
			n.searchReply.DeleteSearchReplyChecker(requestID)
			//receivedData := n.SearchFirstFromNeighbors(pattern, budgetPerNeighbor,
			//	shuffleNeighbors[j], conf.Timeout)

			foundNameFromPeer, isAllChunkAvaliable := n.CheckIfAllChunkAvailable(receivedData)

			if foundNameFromPeer != "" && isAllChunkAvaliable {
				return foundNameFromPeer, nil
			}
		}

	}
	return "", nil

	//return receivedData
}

func (n *node) FindFullTagFromLocal(reg regexp.Regexp) (string, error) {
	resultName := ""

	// suppose local has everything
	localHasEverything := true

	n.conf.Storage.GetNamingStore().ForEach(func(name string, metahash []byte) bool {
		switch reg.MatchString(name) {
		case true:
			// the filename matches -> see if everything is in the node's DataBlobStorage
			metafile := n.conf.Storage.GetDataBlobStore().Get(string(metahash))
			if metafile != nil {
				metafileTrim := bytes.Split(metafile, []byte(peer.MetafileSep))
				for _, chunkHashHex := range metafileTrim {
					if n.conf.Storage.GetDataBlobStore().Get(string(chunkHashHex)) == nil {
						localHasEverything = false
					}
				}
			}

			switch localHasEverything {
			case true:
				resultName = name
				return true
			case false:
				return false
			}

		case false:
			// Do nothing
		}
		return true
	})

	log.Info().Msgf("[FindTagFromLocal] localHasEverything = %v, found name = %v", localHasEverything, resultName)

	return resultName, nil

}

func (n *node) CheckIfAllChunkAvailable(filesInfo []types.FileInfo) (string, bool) {
	isAllChunkAvaliable := true
	foundName := ""

	if filesInfo == nil {
		return "", false
	}

	for _, file := range filesInfo {
		log.Info().Msgf("[CheckIfAllChunkAvailable] filename = %v, chunks = %v", file.Name, file.Chunks)
		for _, chunkhash := range file.Chunks {
			// if there is a nil chunk -> this is not a full match
			if chunkhash == nil {
				isAllChunkAvaliable = false
				break
			}

		}

		if isAllChunkAvaliable {
			foundName = file.Name
			break
		}
		isAllChunkAvaliable = true

	}
	log.Info().Msgf("[CheckIfAllChunkAvailable] foundName %v, isAllChunkAvaliable %v", foundName, isAllChunkAvaliable)

	return foundName, isAllChunkAvaliable
}

/* Main functions for datasharing */

func (n *node) Upload(data io.Reader) (metahash string, err error) {
	// Upload stores a new data blob on the peer and will make it available to
	// other peers. The blob will be split into chunks.

	// Get the storage from n.conf
	nodeStorage := n.conf.Storage.GetDataBlobStore()
	metafile := make([]byte, 0)
	metafileKey := make([]byte, 0)
	chunk := make([]byte, n.conf.ChunkSize)

	for {
		readByte, errRead := data.Read(chunk)

		// Receiving io.EOF
		if readByte == 0 || errRead == io.EOF {
			if len(metafile) > 0 {
				metafile = metafile[:len(metafile)-len([]byte(peer.MetafileSep))]
			}
			break
		}

		chuckHashSlice, chunkHashHex := n.SHA(chunk[:readByte])
		chunkCopy := make([]byte, len(chunk[:readByte]))
		_ = copy(chunkCopy, chunk[:readByte])

		nodeStorage.Set(chunkHashHex, chunkCopy)
		log.Info().Msgf("[Upload] chunkHashHex = %v", chunkHashHex)

		// Store the chunk's hash to metafile
		metafile = append(metafile, chunkHashHex...)
		metafile = append(metafile, []byte(peer.MetafileSep)...)
		metafileKey = append(metafileKey, chuckHashSlice...)
	}
	_, metahash = n.SHA(metafileKey)
	nodeStorage.Set(metahash, metafile)
	log.Info().Msgf("[Upload] metaHashHex = %v", metahash)

	return metahash, nil
}

func (n *node) Download(metahash string) ([]byte, error) {
	// Download will get all the necessary chunks corresponding to the given
	// metahash that references a blob, and return a reconstructed blob. The
	// peer will save locally the chunks that it doesn't have for further
	// sharing. Returns an error if it can't get the necessary chunks.

	metafile := n.conf.Storage.GetDataBlobStore().Get(metahash)
	// metafile = F(c_0)||Sep||F(c_1)||Sep||... -> store the hex_encoding of each chunk
	peersList := n.catalog.FindCatalogEntry(metahash)
	chosenPeer := ""

	if metafile == nil {
		// the node doesn't have the metafile at all
		// -> need to ask the remote peer for both the metafile and the chunk
		switch peersList {
		case nil:
			//log.Error().Msgf("[Download Error] Nobody has the entry")
			return []byte{}, errors.New("neither me nor any peer has the entry")
		}

		chosenPeer = peersList[rand.Int()%(len(peersList))]

		data, err := n.FindMetafileFromPeer(metahash, chosenPeer)
		if err != nil {
			log.Error().Msgf("[Download Error] %v", err)
			return []byte{}, err
		}
		metafile = data

		errChunk := n.FindChunkFromPeer(metafile, chosenPeer)
		if errChunk != nil {
			log.Error().Msgf("[Download Error] %v", errChunk)
			return []byte{}, errChunk
		}
	} else {
		// the node do have the metafile, yet maybe doesn't have all the chunks
		// the chunks may be either in the local storage or the remote peer

		switch peersList {
		case nil:
			chosenPeer = ""
		default:
			chosenPeer = peersList[rand.Int()%(len(peersList))]
		}

		errChunk := n.FindChunkFromPeer(metafile, chosenPeer)
		if errChunk != nil {
			log.Error().Msgf("[Download Error] %v", errChunk)
			return []byte{}, errChunk
		}
	}

	return n.FindChunkFromLocal(metafile)

}

func (n *node) GetCatalog() peer.Catalog {
	// GetCatalog returns the peer's catalog. See below for the definition of a
	// catalog.

	return n.catalog.Freeze()

}

func (n *node) UpdateCatalog(key string, peer string) {
	// UpdateCatalog tells the peer about a piece of data referenced by 'key'
	// being available on other peers. It should update the peer's catalog. See
	// below for the definition of a catalog.

	// Check if the peer is equal to the node itself,
	// if so, don't update the catalog
	if peer != n.conf.Socket.GetAddress() {
		log.Info()
		n.catalog.UpdateCatalogWithMutex(key, peer)
	}

}

func (n *node) Tag(name string, mh string) error {
	// Tag creates a mapping between a (file)name and a metahash

	// If total peer is <= 1 then there is no use of Paxos/TLC/Blockchain.
	if n.conf.TotalPeers <= 1 {
		log.Info().Msgf("[Tag] No Peers\n")
		log.Info().Msgf("[Tag] Store %v:%v in NamingStore", name, mh)
		n.conf.Storage.GetNamingStore().Set(name, []byte(mh))
		return nil
	}
	log.Info().Msgf("[Tag] Total Peer = %v", n.conf.TotalPeers)

	newProposedValue := types.PaxosValue{
		UniqID:   xid.New().String(),
		Filename: name,
		Metahash: mh,
	}

	// Implement Paxos
	decidedValue, err := n.RunPaxosProposer(newProposedValue)
	if err != nil {
		return err
	}

	log.Info().Msgf("[Tag] Store %v:%v in NamingStore", name, mh)
	n.conf.Storage.GetNamingStore().Set(decidedValue.Filename, []byte(decidedValue.Metahash))

	// Question: when will return error??
	return nil
}

func (n *node) Resolve(name string) (metahash string) {
	// Resolve returns the corresponding metahash of a given (file)name. Returns
	// an empty string if not found.

	metahashByte := n.conf.Storage.GetNamingStore().Get(name)
	if metahashByte == nil {
		metahash = ""
	} else {
		metahash = string(metahashByte)
	}

	return metahash
}

func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) (names []string, err error) {
	// SearchAll returns all the names that exist matching the given regex. It
	// merges results from the local storage and from the search request reply
	// sent to a random neighbor using the provided budget. It makes the peer
	// update its catalog and name storage according to the SearchReplyMessages
	// received. Returns an empty result if nothing found. An error is returned
	// in case of an exceptional event.

	// Find all neighbors of this node
	neighbors := n.routingtable.FindNeighbor(n.conf.Socket.GetAddress())
	shuffleNeighbors, budgetPerNeighbor := n.FindBudgetPerNeighbor(neighbors, budget)

	if len(shuffleNeighbors) != 0 {
		switch budgetPerNeighbor {
		case 0:
			for i := 0; i < int(budget); i++ {
				requestID := xid.New().String()
				channel := n.searchReply.InitSearchReplyChecker(requestID)
				log.Info().Msgf("[SearchAll] [%v] searchReq => [%v]", n.conf.Socket.GetAddress(), shuffleNeighbors[i])
				_ = n.SendSearchRequest(reg.String(), uint(1), shuffleNeighbors[i], requestID, n.conf.Socket.GetAddress())

				// Error handling?
				_, _ = n.WaitForSearchReply(timeout, channel)
				n.searchReply.DeleteSearchReplyChecker(requestID)
			}
		default:
			for i := 0; i < len(shuffleNeighbors); i++ {
				if i == len(shuffleNeighbors)-1 {
					// last one -> budget should be the rest
					usedBudget := uint(len(shuffleNeighbors)-1) * budgetPerNeighbor
					budgetPerNeighbor = budget - usedBudget
				}
				requestID := xid.New().String()
				channel := n.searchReply.InitSearchReplyChecker(requestID)
				log.Info().Msgf("[SearchAll] [%v] searchReq => [%v]", n.conf.Socket.GetAddress(), shuffleNeighbors[i])
				_ = n.SendSearchRequest(reg.String(), budgetPerNeighbor, shuffleNeighbors[i], requestID, n.conf.Socket.GetAddress())
				// Error handling?
				_, _ = n.WaitForSearchReply(timeout, channel)
				n.searchReply.DeleteSearchReplyChecker(requestID)
			}

		}

	}
	// no neighbor -> only return the node's matching names

	names, err = n.FindTagFromLocal(reg)

	return names, err
}

func (n *node) SearchFirst(pattern regexp.Regexp, conf peer.ExpandingRing) (name string, err error) {
	// SearchFirst uses an expanding ring configuration and returns a name as
	// soon as it finds a peer that "fully matches" a data blob. It makes the
	// peer update its catalog and name storage according to the
	// SearchReplyMessages received. Returns an empty string if nothing was
	// found.

	// First check if it has everything in local
	if foundName, _ := n.FindFullTagFromLocal(pattern); foundName != "" {
		return foundName, nil
	}

	// need to check the if the neighbors have data

	// Find all neighbors of this node
	neighbors := n.routingtable.FindNeighbor(n.conf.Socket.GetAddress())
	budget := conf.Initial
	for i := 0; i < int(conf.Retry); i++ {

		shuffleNeighbors, budgetPerNeighbor := n.FindBudgetPerNeighbor(neighbors, budget)

		if len(shuffleNeighbors) != 0 {

			foundNameFromPeer, _ := n.SearchFirstFromNeighbors(pattern, budget, budgetPerNeighbor,
				shuffleNeighbors, conf.Timeout)

			if foundNameFromPeer != "" {
				return foundNameFromPeer, nil
			}
		}
		// no neighbor -> only return the node's matching names

		budget = budget * conf.Factor
	}

	return "", nil
}
