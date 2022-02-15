package surfstore

import (
	context "context"
	"fmt"
	"log"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// if err := contextError(ctx); err != nil {
	// 	return nil, err
	// }
	log.Println("get block: ")
	for key, _ := range bs.BlockMap {
		log.Println(key)
	}
	if val, ok := bs.BlockMap[blockHash.GetHash()]; ok {
		return val, nil
	} else {
		return nil, fmt.Errorf("blockHash does not exist in BlockStore\n")
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// if err := contextError(ctx); err != nil {
	// 	res := &surfstore.Success{
	// 		flag: false,
	// 	}
	// 	return res, err
	// }

	hashCode := GetBlockHashString(block.GetBlockData())
	bs.BlockMap[hashCode] = block
	res := &Success{
		Flag: true,
	}

	log.Println("put block: ")
	for key, _ := range bs.BlockMap {
		log.Println(key)
	}
	// log.Printf("block map: %v\n", bs.BlockMap)
	return res, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var found []string
	for _, blockHash := range blockHashesIn.GetHashes() {
		if _, ok := bs.BlockMap[blockHash]; ok {
			found = append(found, blockHash)
		}
	}

	res := &BlockHashes{
		Hashes: found,
	}
	return res, nil
}

// func contextError(ctx context.Context) error {
// 	switch ctx.Err() {
// 	case context.Canceled:
// 		return fmt.Errorf(status.Error(codes.Canceled, "request is canceled"))
// 	case context.DeadlineExceeded:
// 		return fmt.Errorf(status.Error(codes.DeadlineExceeded, "deadline is exceeded"))
// 	default:
// 		return nil
// 	}
// }

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
