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

func (n *node) SHA(chunk []byte) ([]byte, string) {
	// Calculate the hex-encoded chunk’s hash
	h := crypto.SHA256.New()
	h.Write(chunk)
	chuckHashSlice := h.Sum(nil)
	chunkHashHex := hex.EncodeToString(chuckHashSlice)
	return chuckHashSlice, chunkHashHex
}

func (n *node) SendDataRequest(metahash string, chosenPeer string, timeout time.Duration, attempt uint) ([]byte, error) {
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
	n.replyRecord.OpenReplyChecker(requestID)

	select {
	case data := <-n.replyRecord.FindReplyEntry(requestID):
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
		if peersList == nil {
			log.Error().Msgf("[Download Error] Nobody has the entry")
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

		// Question: There may be some error on not setting the chosenPeer??
		if peersList == nil {
			chosenPeer = ""
		} else {
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
	log.Info().Msgf("[UpdateCatalog] Update Catalog of node [%v], key = %v, peer = %v", n.conf.Socket.GetAddress(), key, peer)
	if peer != n.conf.Socket.GetAddress() {
		log.Info()
		n.catalog.UpdateCatalogWithMutex(key, peer)
	}

}

func (n *node) Tag(name string, mh string) error {
	// Tag creates a mapping between a (file)name and a metahash
	log.Info().Msgf("[Tag] Store %v:%v in NamingStore", name, mh)
	n.conf.Storage.GetNamingStore().Set(name, []byte(mh))

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

	tmpString := make([]string, 0)

	return tmpString, nil
}
