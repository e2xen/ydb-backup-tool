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

	// Perform full backup of YDB
	var ydbCmd *exec.Cmd
	// TODO: verify correct priority if multiple auth methods are passed
	// TODO: possible to refactor using vararg array
	if params.YcTokenFile != "" {
		// TODO: maybe check that params.YcTokenFile exists
		ydbCmd = exec.Command(ydbPath, "-e", params.Endpoint, "-d", params.Name, "--yc-token-file", params.YcTokenFile, "tools", "dump", "-o", path)
	} else if params.IamTokenFile != "" {
		// TODO: maybe check that params.IamTokenFile exiSsts
		ydbCmd = exec.Command(ydbPath, "-e", params.Endpoint, "-d", params.Name, "--iam-token-file", params.IamTokenFile, "tools", "dump", "-o", path)
	} else if params.SaKeyFile != "" {
		// TODO: maybe check that params.SaKeyFile exists
		ydbCmd = exec.Command(ydbPath, "-e", params.Endpoint, "-d", params.Name, "--sa-key-file", params.SaKeyFile, "tools", "dump", "-o", path)
	} else if params.Profile != "" {
		ydbCmd = exec.Command(ydbPath, "-e", params.Endpoint, "-d", params.Name, "-p", params.Profile, "tools", "dump", "-o", path)
	} else if params.UseMetadataCreds {
		ydbCmd = exec.Command(ydbPath, "-e", params.Endpoint, "-d", params.Name, "--use-metadata-credentials", "tools", "dump", "-o", path)
	} else {
		ydbCmd = exec.Command(ydbPath, "-e", params.Endpoint, "-d", params.Name, "tools", "dump", "-o", path)
	}

	if err := ydbCmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to perform YDB dump")
	}

	return &BackupInfo{Path: path}, nil
}
