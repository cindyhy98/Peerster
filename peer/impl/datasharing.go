package impl

import (
	"crypto"
	"encoding/hex"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"io"
)

func (n *node) SHA(chunk []byte) ([]byte, string) {
	// Calculate the hex-encoded chunk’s hash
	h := crypto.SHA256.New()
	h.Write(chunk)
	chuckHashSlice := h.Sum(nil)
	chunkHashHex := hex.EncodeToString(chuckHashSlice)
	return chuckHashSlice, chunkHashHex
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

	return metahash, nil
}

func (n *node) Download(metahash string) ([]byte, error) {
	// Download will get all the necessary chunks corresponding to the given
	// metahash that references a blob, and return a reconstructed blob. The
	// peer will save locally the chunks that it doesn't have for further
	// sharing. Returns an error if it can't get the necessary chunks.

	return []byte{}, nil
}
