package meta

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"
	_const "ydb-backup-tool/internal/const"
	"ydb-backup-tool/internal/utils"
)

type BtrfsNode struct {
	Backups []Backup `json:"backups"`
}

type Backup struct {
	Completed          bool       `json:"completed"`
	Path               string     `json:"path"`
	StartedCreationAt  time.Time  `json:"started_creation_at"`
	FinishedCreationAt *time.Time `json:"finished_creation_at"`
}

type metaFileStructure struct {
	Btrfs BtrfsNode `json:"btrfs"`
}

func StartBackup(path string) error {
	btrfsNode, err := GetBtrfsNode()
	if err != err {
		return fmt.Errorf("failed to get current backups meta info: %s", err)
	}

	for _, backup := range btrfsNode.Backups {
		if backup.Path == path {
			return fmt.Errorf("cannot add backup %s since it already exists in the meta file", path)
		}
	}

	(*btrfsNode).Backups = append((*btrfsNode).Backups, Backup{
		Completed:         false,
		Path:              path,
		StartedCreationAt: time.Now(),
	})

	if err := saveStateToFile(&metaFileStructure{Btrfs: *btrfsNode}); err != nil {
		return err
	}

	return nil
}

func FinishBackup(path string) error {
	btrfsNode, err := GetBtrfsNode()
	if err != err {
		return fmt.Errorf("failed to get current backups meta info: %s", err)
	}

	for i := range btrfsNode.Backups {
		if btrfsNode.Backups[i].Path == path {
			btrfsNode.Backups[i].Completed = true
			now := time.Now()
			btrfsNode.Backups[i].FinishedCreationAt = &now
		}
	}

	if err := saveStateToFile(&metaFileStructure{Btrfs: *btrfsNode}); err != nil {
		return err
	}

	return nil
}

func GetBtrfsNode() (*BtrfsNode, error) {
	f, err := getOrCreateMetaFile(os.O_RDONLY, os.ModeType)
	if err != nil {
		return nil, err
	}

	r := bufio.NewReader(f)
	buff, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read from the meta file %s: %s", _const.AppMetaPath, err)
	}

	var metaFileStruct metaFileStructure
	if err := json.Unmarshal(buff, &metaFileStruct); err != nil {
		return nil, fmt.Errorf("failed to parse JSON object from the meta file %s: %s", _const.AppMetaPath, err)
	}

	return &metaFileStruct.Btrfs, nil
}

func GetBackups() (*[]Backup, error) {
	btrfsNode, err := GetBtrfsNode()
	if err != nil {
		return nil, err
	}

	return &btrfsNode.Backups, nil
}

func getOrCreateMetaFile(flag int, perm os.FileMode) (*os.File, error) {
	if _, err := os.Stat(_const.AppMetaPath); os.IsNotExist(err) {
		if err := createMetaFile(); err != nil {
			return nil, fmt.Errorf("failed to create meta file: %s", err)
		}
	}

	return os.OpenFile(_const.AppMetaPath, flag, perm)
}

func createMetaFile() error {
	if err := utils.CreateFile(_const.AppMetaPath); err != nil {
		return fmt.Errorf("failed to create file %s for meta storage: %s", _const.AppMetaPath, err)
	}

	if err := saveStateToFile(&metaFileStructure{}); err != nil {
		return err
	}

	return nil
}

func saveStateToFile(metaFileStructure *metaFileStructure) error {
	f, err := os.OpenFile(_const.AppMetaPath, os.O_CREATE|os.O_WRONLY, os.ModeExclusive)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		if err := f.Close(); err != nil {
			log.Printf("WARN: failed to close descriptor of the file %s", f.Name())
		}
	}(f)

	jsonByte, _ := json.Marshal(metaFileStructure)
	w := bufio.NewWriterSize(f, len(jsonByte))
	if _, err = w.Write(jsonByte); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}

	return nil
}
