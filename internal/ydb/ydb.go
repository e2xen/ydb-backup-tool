package ydb

import (
	"fmt"
	"strconv"
	"ydb-backup-tool/internal/utils"
)

type YdbParams struct {
	Endpoint         string
	Name             string
	YcTokenFile      string
	IamTokenFile     string
	SaKeyFile        string
	Profile          string
	UseMetadataCreds bool
}

type DumpParams struct {
	Path             string
	Exclude          string
	ConsistencyLevel string
	AvoidCopy        bool
	SchemeOnly       bool
}

type RestoreParams struct {
	Path    string
	Data    uint64
	Indexes uint64
	DryRun  bool
}

type Backup struct {
	Path string
}

func Dump(ydbParams *YdbParams, dumpParams *DumpParams, path string) (*Backup, error) {
	ydbPath, err := utils.GetBinary("ydb")
	if err != nil {
		return nil, err
	}

	args := []string{"-e", ydbParams.Endpoint, "-d", ydbParams.Name}
	args = addAuthParams(ydbParams, args)
	args = append(args, "tools", "dump", "-o", path, "-p", dumpParams.Path,
		"--consistency-level", dumpParams.ConsistencyLevel)
	if dumpParams.Exclude != "" {
		args = append(args, "--exclude", dumpParams.Exclude)
	}
	if dumpParams.AvoidCopy {
		args = append(args, "--avoid-copy")
	}
	if dumpParams.SchemeOnly {
		args = append(args, "--scheme-only")
	}

	// Perform full backup of YDB
	ydbCmd := utils.BuildCommand(ydbPath, args...)
	if err := ydbCmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to perform YDB dump")
	}

	return &Backup{Path: path}, nil
}

func Restore(ydbParams *YdbParams, restoreParams *RestoreParams, sourcePath string) error {
	ydbPath, err := utils.GetBinary("ydb")
	if err != nil {
		return err
	}

	args := []string{"-e", ydbParams.Endpoint, "-d", ydbParams.Name}
	args = addAuthParams(ydbParams, args)
	args = append(args, "tools", "restore", "-p", restoreParams.Path, "-i", sourcePath,
		"--restore-data", strconv.FormatUint(restoreParams.Data, 10),
		"--restore-indexes", strconv.FormatUint(restoreParams.Indexes, 10))
	if restoreParams.DryRun {
		args = append(args, "--dry-run")
	}

	// Perform restore of YDB
	ydbCmd := utils.BuildCommand(ydbPath, args...)
	if err := ydbCmd.Run(); err != nil {
		return fmt.Errorf("failed to restore YDB from the backup `%s`", sourcePath)
	}

	return nil
}

func addAuthParams(ydbParams *YdbParams, args []string) []string {
	if ydbParams.YcTokenFile != "" {
		args = append(args, "--yc-token-file", ydbParams.YcTokenFile)
	}
	if ydbParams.IamTokenFile != "" {
		args = append(args, "--iam-token-file", ydbParams.IamTokenFile)
	}
	if ydbParams.SaKeyFile != "" {
		args = append(args, "--sa-key-file", ydbParams.SaKeyFile)
	}
	if ydbParams.Profile != "" {
		args = append(args, "-p", ydbParams.Profile)
	}
	if ydbParams.UseMetadataCreds {
		args = append(args, "--use-metadata-credentials")
	}
	return args
}
