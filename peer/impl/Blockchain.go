package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/types"
	"strconv"
)

func (n *node) CalculateHash(prevHash []byte, value types.PaxosValue) ([]byte, string) {

	blockchainInfo := strconv.Itoa(int(n.tlcCurrentState.currentLogicalClock)) +
		value.UniqID +
		value.Filename +
		value.Metahash +
		string(prevHash)

	blockchainHashSlice, blockchainHashHex := n.SHA([]byte(blockchainInfo))

	return blockchainHashSlice, blockchainHashHex
}

func (n *node) BuildBlock(thisStep uint, prevHash []byte, acceptedValue types.PaxosValue) types.BlockchainBlock {

	if prevHash == nil {
		prevHash = make([]byte, 32)
	}

	blockchainHashSlice, _ := n.CalculateHash(prevHash, acceptedValue)

	newBlockchainBlock := types.BlockchainBlock{
		Index:    thisStep,
		Hash:     blockchainHashSlice,
		Value:    acceptedValue,
		PrevHash: prevHash,
	}

	return newBlockchainBlock
}

func (n *node) AddBlock(block types.BlockchainBlock) error {

	// Marshal blockchain
	blockByte, err := block.Marshal()
	if err != nil {
		log.Error().Msgf("[BuildBlockchain] %v", err)
		return err
	}

	log.Info().Msgf("[AddBlock]")

	_, blockHashHex := n.CalculateHash(block.PrevHash, block.Value)

	// Store this block by its hex-encoded hash value
	n.conf.Storage.GetBlockchainStore().Set(blockHashHex, blockByte)

	// Store the last block hash with the key
	n.conf.Storage.GetBlockchainStore().Set(storage.LastBlockKey, block.Hash)

	return nil

}
