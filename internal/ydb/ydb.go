package ydb

import (
	"fmt"
	"os/exec"
	"ydb-backup-tool/internal/utils"
)

type Params struct {
	Endpoint         string
	Name             string
	YcTokenFile      string
	IamTokenFile     string
	SaKeyFile        string
	Profile          string
	UseMetadataCreds bool
}

type BackupInfo struct {
	Path string
}

func Dump(params *Params, path string) (*BackupInfo, error) {
	// TODO: add search of binary is user's profile directory if running as sudo
	ydbPath, err := utils.GetBinary("ydb")
	if err != nil {
		return nil, err
	}

	// TODO: verify YDB connection(for example, discovery whoami). However, discovery works only with --yc-token-file

	args := []string{"-e", params.Endpoint, "-d", params.Name}
	if params.YcTokenFile != "" {
		// TODO: maybe check that params.YcTokenFile exists
		args = append(args, "--yc-token-file", params.YcTokenFile)
	}
	if params.IamTokenFile != "" {
		// TODO: maybe check that params.IamTokenFile exists
		args = append(args, "--iam-token-file", params.IamTokenFile)
	}
	if params.SaKeyFile != "" {
		// TODO: maybe check that params.SaKeyFile exists
		args = append(args, "--sa-key-file", params.SaKeyFile)
	}
	if params.Profile != "" {
		args = append(args, "-p", params.Profile)
	}
	if params.UseMetadataCreds {
		args = append(args, "--use-metadata-credentials")
	}
	args = append(args, "tools", "dump", "-o", path)

	// Perform full backup of YDB
	ydbCmd := exec.Command(ydbPath, args...)
	println(ydbCmd.String())
	if err := ydbCmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to perform YDB dump")
	}

	return &BackupInfo{Path: path}, nil
}
