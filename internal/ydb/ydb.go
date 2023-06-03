package ydb

import (
	"fmt"
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

type Backup struct {
	Path string
}

func Dump(params *Params, path string) (*Backup, error) {
	// TODO: add search of binary is user's profile directory if running as sudo
	ydbPath, err := utils.GetBinary("ydb")
	if err != nil {
		return nil, err
	}

	// TODO: verify YDB connection(for example, discovery whoami). However, discovery works only with token creds
	args := []string{"-e", params.Endpoint, "-d", params.Name}
	args = addAuthParams(params, args)
	args = append(args, "tools", "dump", "-o", path)

	// Perform full backup of YDB
	ydbCmd := utils.BuildCommand(ydbPath, args...)
	if err := ydbCmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to perform YDB dump")
	}

	return &Backup{Path: path}, nil
}

func Restore(params *Params, sourcePath string) error {
	// TODO: add search of binary is user's profile directory if running as sudo
	ydbPath, err := utils.GetBinary("ydb")
	if err != nil {
		return err
	}

	args := []string{"-e", params.Endpoint, "-d", params.Name}
	args = addAuthParams(params, args)
	args = append(args, "tools", "restore", "-p", ".", "-i", sourcePath)

	// Perform restore of YDB
	ydbCmd := utils.BuildCommand(ydbPath, args...)
	if err := ydbCmd.Run(); err != nil {
		return fmt.Errorf("failed to restore YDB from the backup `%s`", sourcePath)
	}

	return nil
}

func addAuthParams(params *Params, args []string) []string {
	if params.YcTokenFile != "" {
		args = append(args, "--yc-token-file", params.YcTokenFile)
	}
	if params.IamTokenFile != "" {
		args = append(args, "--iam-token-file", params.IamTokenFile)
	}
	if params.SaKeyFile != "" {
		args = append(args, "--sa-key-file", params.SaKeyFile)
	}
	if params.Profile != "" {
		args = append(args, "-p", params.Profile)
	}
	if params.UseMetadataCreds {
		args = append(args, "--use-metadata-credentials")
	}
	return args
}
