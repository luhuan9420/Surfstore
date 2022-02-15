package surfstore

import (
	context "context"
	"fmt"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	res := &FileInfoMap{
		FileInfoMap: m.FileMetaMap,
	}
	return res, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	filename := fileMetaData.GetFilename()
	// check if file exists in the map
	if _, ok := m.FileMetaMap[filename]; ok {
		curr := m.FileMetaMap[filename].GetVersion()
		new := fileMetaData.GetVersion()
		if new-curr == 1 {
			m.FileMetaMap[filename] = fileMetaData
			return &Version{Version: fileMetaData.GetVersion()}, nil
		} else {
			return &Version{Version: -1}, fmt.Errorf("New version number is not right\n")
		}
	} else {
		m.FileMetaMap[filename] = fileMetaData
		return &Version{Version: fileMetaData.GetVersion()}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	// log.Printf("block store address: %v\n", m.BlockStoreAddr)
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	log.Printf("block store addr: %v\n", blockStoreAddr)
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
