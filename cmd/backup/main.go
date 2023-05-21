package main

import (
	"flag"
	"log"
	"strings"
	"ydb-backup-tool/internal/btrfs"
	cmd "ydb-backup-tool/internal/command"
	_const "ydb-backup-tool/internal/const"
	"ydb-backup-tool/internal/device"
	"ydb-backup-tool/internal/ydb"
)

var (
	ydbEndpoint     *string
	ydbName         *string
	ydbYcTokenFile  *string
	ydbIamTokenFile *string
	ydbSaKeyFile    *string
	ydbProfile      *string
)

func init() {
	ydbEndpoint = flag.String(_const.YdbEndpointArg, "", "YDB endpoint")
	ydbName = flag.String(_const.YdbNameArg, "", "YDB database name")
	ydbYcTokenFile = flag.String(_const.YdbYcTokenFileArg, "", "YDB OAuth token file")
	ydbIamTokenFile = flag.String(_const.YdbIamTokenFileArg, "", "YDB IAM token file")
	ydbSaKeyFile = flag.String(_const.YdbSaKeyFileArg, "", "YDB Service Account Key file")
	ydbProfile = flag.String(_const.YdbProfileArg, "", "YDB profile name")
	flag.Bool(_const.YdbUseMetadataCredsArg, false, "YDB use the metadata service")
}

func isArgFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func parseAndValidateArgs() *cmd.Command {
	// TODO: configuration through env
	flag.Parse()

	if strings.TrimSpace(*ydbEndpoint) == "" {
		log.Panic("You need to specify YDB url passing the following parameter: \"--ydb-endpoint=<url>\"")
	}
	if strings.TrimSpace(*ydbName) == "" {
		log.Panic("You need to specify YDB database name passing the following parameter: \"--ydb-name=<name>\"")
	}
	if len(flag.Args()) == 0 {
		log.Panic("You need to pass command")
	}

	var command cmd.Command
	switch strings.TrimSpace(flag.Arg(0)) {
	case "list-sizes":
		command = cmd.ListAllBackupsSizes
	case "ls", "list":
		command = cmd.ListAllBackups
		break
	case "create-full":
		command = cmd.CreateFullBackup
		break
	case "create-inc":
		command = cmd.CreateIncrementalBackup
		break
	case "restore":
		command = cmd.RestoreFromBackup
		break
	default:
		command = cmd.Undefined
	}

	if command == cmd.Undefined {
		log.Panicf("Could not parse command")
	}

	return &command
}

func main() {
	command := parseAndValidateArgs()

	// TODO: check if running as sudo

	// TODO: add "--help" option
	// TODO: add "--debug" option

	// TODO: is is ok that we have only one .img file for all backups(for example, data.img)? Allow users to specify base filename through args?
	backingFilePath := _const.AppBaseDataBackingFilePath
	// Verify img file exists or create it in case of absence
	backingFile, created, err := device.GetOrCreateBackingStoreFile(backingFilePath)
	if err != nil {
		log.Panicf("Cannot obtain backing file")
	}
	if created {
		if err := btrfs.MakeBtrfsFileSystem(backingFile.Path); err != nil {
			log.Panicf("Failted to make Btrfs file system")
		}
	}

	loopDev, err := device.SetupLoopDevice(backingFile)
	if err != nil {
		log.Panicf("Cannot create loop device. %v", err)
	}
	defer func(loopDevice *device.LoopDevice) {
		err := device.DetachLoopDevice(loopDevice)
		if err != nil {
			log.Panicf("WARN: cannot detach the loop device.")
		}
	}(loopDev)

	mountPoint, err := device.MountLoopDevice(loopDev, _const.AppDataMountPath)
	if err != nil {
		log.Panicf("Cannot mount the backing file. %v", err)
	}
	defer func(mountPoint *device.MountPoint) {
		err := device.Unmount(mountPoint)
		if err != nil {
			log.Printf("WARN: cannot unmount the backing file.")
		}
	}(mountPoint)

	switch *command {
	case cmd.ListAllBackups:
		err := command.ListBackups(mountPoint)
		if err != nil {
			log.Panicf("Cannot list backups because of the following error: %v", err)
		}
		break
	case cmd.ListAllBackupsSizes:
		err := command.ListBackupsSizes(mountPoint)
		if err != nil {
			log.Panicf("Cannot list backup sizes: %v", err)
		}
	case cmd.CreateFullBackup:
		ydbParams := initYdbParams()
		if err := command.CreateFullBackup(mountPoint, ydbParams); err != nil {
			log.Panicf("Cannot perform full backup: %v", err)
		}
		break
	case cmd.CreateIncrementalBackup:
		ydbParams := initYdbParams()
		if err := command.CreateIncrementalBackup(mountPoint, ydbParams); err != nil {
			log.Panicf("Cannot perform incremental backup: %v", err)
		}
		break
	case cmd.RestoreFromBackup:
		if len(flag.Args()) <= 1 {
			log.Panic("You should specify backup name: restore <name>")
		}

		sourcePath := flag.Arg(1)
		ydbParams := initYdbParams()
		if err := command.RestoreFromBackup(mountPoint, ydbParams, sourcePath); err != nil {
			log.Panicf("Cannot restore from the backup: %v", err)
		}
		break
	}
}

func initYdbParams() *ydb.Params {
	return &ydb.Params{Endpoint: *ydbEndpoint,
		Name:             *ydbName,
		YcTokenFile:      *ydbYcTokenFile,
		IamTokenFile:     *ydbIamTokenFile,
		SaKeyFile:        *ydbSaKeyFile,
		Profile:          *ydbProfile,
		UseMetadataCreds: isArgFlagPassed(_const.YdbUseMetadataCredsArg),
	}
}
