package dump

import (
	"log"
	"os/exec"
)

type YdbParams struct {
	DbUrl  *string
	DbName *string
}

// TODO: change contract
// TODO: use *string or just string?
type BackupInfo struct {
	Path    string
	Fileame string
}

func YdbDump(ydbParams *YdbParams, path string) (*BackupInfo, error) {
	// TODO: add search is user's profile directory if running as sudo
	// TODO: verify YDB connection stage
	ydbPath, err := exec.LookPath("ydb")
	if err != nil {
		log.Fatal("YDB CLI is not found in %PATH%")
	}

	// Perform full backup of YDB
	ydbCmd := exec.Command(ydbPath, "-e", *ydbParams.DbUrl, "-d", *ydbParams.DbName, "tools", "dump", "-o", path)
	if err := ydbCmd.Run(); err != nil {
		return nil, err
	}

	return &BackupInfo{Path: path}, nil
}
