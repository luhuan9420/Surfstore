package surfstore

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// check if base dir is valid
	baseDir := client.BaseDir
	files, err := ioutil.ReadDir(baseDir)
	if err != nil {
		fmt.Errorf("Error when trying to read client base directory: %v", err)
	}

	// check index.txt file
	indexPath := client.BaseDir + "/index.txt"
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		file, err := os.Create(indexPath)
		if err != nil {
			log.Fatal("Fail to create index file: %v", err)
		}
		defer file.Close()
	}

	// local index.txt file info
	// localFileInfoMap := make(map[string]FileMetaData)
	// localIndex, err := ioutil.ReadFile(indexPath)
	// if err != nil {
	// 	log.Fatal("Fail to read index file: %v", err)
	// }
	// localIndexLines := strings.Split(string(localIndex), "\n")
	// for _, line := range localIndexLines {
	// 	if len(line) == 0 {
	// 		continue
	// 	}
	// 	// lineArr := strings.Split(line, ",")
	// 	// if len(lineArr) != 3 {
	// 	// 	fmt.Printf("Invalid format line: %v", line)
	// 	// 	continue
	// 	// }
	// 	// filename := lineArr[0]
	// 	// version, _ := strconv.Atoi(lineArr[1])
	// 	// hashList := strings.Split(lineArr[2], " ")
	// 	// fmd := &FileMetaData {
	// 	// 	Filename: filename,
	// 	// 	Version: version,
	// 	// 	HaBlockHashList: hashList,
	// 	// }
	// 	fmd := NewFileMetaDataFromConfig(line)
	// 	localFileInfoMap[fmd.GetFilename()] = fmd
	// }
	localFileInfoMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Fatal("Error when trying to load local file info map: %v", err)
	}

	fileDelete := make(map[string]bool)
	for filename, _ := range localFileInfoMap {
		fileDelete[filename] = false
	}
	/* compare dir files with index.txt
	1. check if there is new files in dir that are not in index.txt
	2. check if any file in index.txt hash list is different (have changed since last time client sync)
	*/
	// fileModified := make(map[string]string)
	fileModified := make(map[string]bool)
	// fileNew := make(map[string]string)
	for _, f := range files {
		if f.Name() == "index.txt" {
			continue
		}
		file, err := os.Open(client.BaseDir + "/" + f.Name())
		if err != nil {
			log.Fatal("Fail to open file %v: %v", f.Name(), err)
		}
		fileSize := f.Size()
		blockNeeded := int(math.Ceil(float64(fileSize)) / float64(client.BlockSize))

		// file in index.txt, check if hash list is different
		if fmd, ok := localFileInfoMap[f.Name()]; ok {
			delete(fileDelete, f.Name())
			thisHashList := make([]string, blockNeeded)
			changed := false
			for i := 0; i < blockNeeded; i++ {
				buf := make([]byte, client.BlockSize)
				size, err := file.Read(buf)
				if err != nil {
					fmt.Printf("error when trying to get hash list: ", err)
				}
				buf = buf[:size]

				hashString := GetBlockHashString(buf)
				thisHashList[i] = hashString
				if i >= len(fmd.GetBlockHashList()) || hashString != fmd.GetBlockHashList()[i] {
					changed = true
				}
			}
			if blockNeeded != len(fmd.GetBlockHashList()) {
				changed = true
			}
			// hashStr := GetHashString(thisHashList)
			if changed {
				localFileInfoMap[f.Name()].BlockHashList = thisHashList
				//fileModified[f.Name()] = f.Name() + "," + strconv.Itoa(fmd.GetVersion()) + "," + hashStr
				fileModified[f.Name()] = true
			}
		} else {
			// file in dir not in index.txt
			thisHashList := make([]string, blockNeeded)
			for i := 0; i < blockNeeded; i++ {
				buf := make([]byte, client.BlockSize)
				size, err := file.Read(buf)
				if err != nil {
					fmt.Printf("error when trying to get hash list: ", err)
				}
				buf = buf[:size]

				hashString := GetBlockHashString(buf)
				thisHashList[i] = hashString
			}
			// hashStr := GetHashString(thisHashList)
			// fileNew[f.Name()] = f.Name() + "," + strconv.Itoa(1) + "," + hashStr
			fmd := &FileMetaData{
				Filename:      f.Name(),
				Version:       1,
				BlockHashList: thisHashList,
			}
			localFileInfoMap[f.Name()] = fmd
		}
	}
	// remaining key in fileDelete is the file that is deleted by client
	for filename, _ := range fileDelete {
		fmd := &FileMetaData{
			Filename:      filename,
			Version:       localFileInfoMap[filename].GetVersion() + 1,
			BlockHashList: []string{"0"},
		}
		localFileInfoMap[filename] = fmd
		fileModified[filename] = true
	}

	// connect to server
	// download updated FileInfoMap
	serverFileInfoMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&serverFileInfoMap)
	if err != nil {
		fmt.Errorf("Error when trying to get file info from server: %v", err)
	}

	/* compare local index to remote index
	1. remote index refers to a file not present in local index or base dir
	1.1 download blocks associated with that file
	1.2 reconstitute file in base dir
	1.3 add updated FileInfo to local index.txt

	2. new file in local base dir not in local index.txt nor remote index.txt
	2.1 upload blocks corresponding to this file to server
	2.2 update server with new FileInfo
	2.3 update local index if success
	*** might need to handle version conflict ***
	*/
	for fileName, fmd := range localFileInfoMap {
		if serverMD, ok := serverFileInfoMap[fileName]; ok {
			modified := fileModified[fileName]
			if fmd.GetVersion() == serverMD.GetVersion() && !modified {
				continue
			} else if (fmd.GetVersion() > serverMD.GetVersion()) || (fmd.GetVersion() == serverMD.GetVersion() && modified) {
				// server side file is old (or client side file is updated)
				serverSideUpdate(client, fmd, modified, &localFileInfoMap)
			} else {
				// client side file is old
				cleintSideUpdate(client, serverMD, &localFileInfoMap)
			}
		} else {
			uploadNew(client, fmd, &localFileInfoMap)
		}
	}
	// for fileName, fmd := range fileNew {
	// 	if serverMD, ok := serverFileInfoMap[fileName]; ok {
	// 		clientSideUpdate(client, fmd, false, )
	// 	} else {
	// 		uploadNew(client, fmd)
	// 	}
	// }

	// download new files from server
	for filename, serverMD := range serverFileInfoMap {
		if _, exist := localFileInfoMap[filename]; !exist {
			localMD, err := download(client, filename, serverMD)
			if err != nil {
				fmt.Printf("Failed to download file from server: %v", err)
			}
			localFileInfoMap[filename] = localMD
		}
	}

	err = WriteMetaFile(localFileInfoMap, client.BaseDir)
	if err != nil {
		log.Fatal("Fail to update index.txt: %v", err)
	}

	// handle conflict
}

func GetHashString(hashList []string) string {
	hashStr := ""
	for i, hash := range hashList {
		hashStr += hash
		if i != (len(hashList) - 1) {
			hashStr += " "
		}
	}
	return hashStr
}

func serverSideUpdate(client RPCClient, clientMD *FileMetaData, modified bool, localFileInfoMap *map[string]*FileMetaData) {
	// if client file has been updated, version needs to be udpated
	if modified {
		clientMD.Version += 1
		(*localFileInfoMap)[clientMD.GetFilename()] = clientMD
	}
	err := uploadNew(client, clientMD, localFileInfoMap)
	if err != nil {
		fmt.Printf("Fail to upload file: %v", err)
	}
}

func cleintSideUpdate(client RPCClient, serverMD *FileMetaData, localFileInfoMap *map[string]*FileMetaData) {
	downloadMD, err := download(client, serverMD.GetFilename(), serverMD)

	if err != nil {
		fmt.Printf("Fail to download file from server: %v", err)
	}
	(*localFileInfoMap)[serverMD.GetFilename()] = downloadMD
}

func uploadNew(client RPCClient, fmd *FileMetaData, localFileInfoMap *map[string]*FileMetaData) error {
	filePath := client.BaseDir + "/" + fmd.GetFilename()

	if _, e := os.Stat(filePath); os.IsNotExist(e) {
		version := fmd.GetVersion()
		err := client.UpdateFile(fmd, &version)
		if err != nil {
			fmt.Printf("Failed to update file: %v", err)
		}
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Fail to open file: %v", err)
	}
	defer file.Close()

	f, _ := os.Stat(filePath)
	blockNeeded := int(math.Ceil(float64(f.Size())) / float64(client.BlockSize))

	for i := 0; i < blockNeeded; i++ {
		var block Block
		block.BlockData = make([]byte, client.BlockSize)
		n, err := file.Read(block.BlockData)
		if err != nil && err != io.EOF {
			fmt.Printf("Fail to read file: ", err)
		}
		block.BlockSize = int32(n)
		block.BlockData = block.BlockData[:n]

		var blockStoreAddr string
		err = client.GetBlockStoreAddr(&blockStoreAddr)
		if err != nil {
			fmt.Printf("Fail to get block store address: %v", err)
		}
		var succ bool
		err = client.PutBlock(&block, blockStoreAddr, &succ)
		if err != nil {
			fmt.Printf("Fail to put block: %v", err)
		}
	}

	version := fmd.GetVersion()
	err = client.UpdateFile(fmd, &version)
	if err != nil {
		// version mismatch
		fmt.Printf("Failed to update file: %v", err)
		// download newest files from server
		serverFileInfoMap := make(map[string]*FileMetaData)
		err = client.GetFileInfoMap(&serverFileInfoMap)
		if err != nil {
			fmt.Printf("Fail to get file info: %v", err)
		}
		cleintSideUpdate(client, serverFileInfoMap[fmd.GetFilename()], localFileInfoMap)
	}
	return err
}

func download(client RPCClient, filename string, serverMD *FileMetaData) (*FileMetaData, error) {
	filePath := client.BaseDir + "/" + filename
	if _, e := os.Stat(filePath); os.IsNotExist(e) {
		os.Create(filePath)
	} else {
		err := os.Truncate(filePath, 0)
		if err != nil {
			fmt.Printf("Fail to truncate the file: %v", err)
		}
	}

	// file is deleted in server
	if len(serverMD.GetBlockHashList()) == 1 && serverMD.GetBlockHashList()[0] == "0" {
		err := os.Remove(filePath)
		if err != nil {
			fmt.Printf("Fail to delete file: %v", err)
		}
		return serverMD, err
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		fmt.Printf("Fail to open file: %v", err)
	}
	defer file.Close()

	var blockStoreAddr string
	err = client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		fmt.Printf("Fail to get block store address: %v", err)
	}

	for _, hash := range serverMD.GetBlockHashList() {
		var block Block
		err := client.GetBlock(hash, blockStoreAddr, &block)
		if err != nil {
			fmt.Printf("Fail to get block: %v", err)
		}

		data := string(block.BlockData)
		_, err = io.WriteString(file, data)
		if err != nil {
			fmt.Printf("Fail to write file: ", err)
		}
	}
	fmd := &FileMetaData{
		Filename:      serverMD.GetFilename(),
		Version:       serverMD.GetVersion(),
		BlockHashList: serverMD.GetBlockHashList(),
	}
	return fmd, err
}