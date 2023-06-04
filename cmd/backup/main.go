package main

import (
	"flag"
	"log"
	"strings"
	"ydb-backup-tool/internal/btrfs"
	comp "ydb-backup-tool/internal/btrfs/compression"
	cmd "ydb-backup-tool/internal/command"
	_const "ydb-backup-tool/internal/const"
	"ydb-backup-tool/internal/device"
	"ydb-backup-tool/internal/utils"
	"ydb-backup-tool/internal/ydb"
)

var (
	ydbEndpoint          *string
	ydbName              *string
	ydbYcTokenFile       *string
	ydbIamTokenFile      *string
	ydbSaKeyFile         *string
	ydbProfile           *string
	compressionAlgorithm *string
	compressionLevel     *uint64
	compression          *comp.Compression
)

func init() {
	ydbEndpoint = flag.String(_const.YdbEndpointArg, "", "YDB endpoint")
	ydbName = flag.String(_const.YdbNameArg, "", "YDB database name")
	ydbYcTokenFile = flag.String(_const.YdbYcTokenFileArg, "", "YDB OAuth token file")
	ydbIamTokenFile = flag.String(_const.YdbIamTokenFileArg, "", "YDB IAM token file")
	ydbSaKeyFile = flag.String(_const.YdbSaKeyFileArg, "", "YDB Service Account Key file")
	ydbProfile = flag.String(_const.YdbProfileArg, "", "YDB profile name")
	compressionAlgorithm = flag.String(_const.CompressionAlgorithmArg, "", "Compression algorithm")
	compressionLevel = flag.Uint64(_const.CompressionLevelArg, 1, "Compression level")

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
	flag.Parse()

	if strings.TrimSpace(*ydbEndpoint) == "" {
		log.Panic("You need to specify YDB url passing the following parameter: \"--ydb-endpoint=<url>\"")
	}
	if strings.TrimSpace(*ydbName) == "" {
		log.Panic("You need to specify YDB database name passing the following parameter: \"--ydb-name=<name>\"")
	}
	if strings.TrimSpace(*compressionAlgorithm) != "" {
		compressionAlgorithm := strings.ToLower(strings.TrimSpace(*compressionAlgorithm))
		compressionObj, err := comp.CreateCompression(comp.Algorithm(compressionAlgorithm), *compressionLevel)
		if err != nil {
			log.Panicf("Failed to parse compression parameters: %s", err)
		}

		compression = &compressionObj
	}
	if len(flag.Args()) == 0 {
		log.Panic("You need to pass a command")
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
		log.Panicf("Could not parse command")
	}

	return &command
}

func main() {
	command := parseAndValidateArgs()

	// TODO: check if running as sudo

	// TODO: add "--help" option
	// TODO: add "--debug" option

	backingFilePath := _const.AppBaseDataBackingFilePath
	// Verify img file exists or create it in case of absence
	backingFile, created, err := device.GetOrCreateBackingStoreFile(backingFilePath)
	if err != nil {
		log.Panicf("Cannot obtain backing file")
	}
	if created {
		if err := btrfs.MakeBtrfsFileSystem(backingFile.Path); err != nil {
			log.Panicf("Failed to make Btrfs")
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

	mountPoint, err := device.MountLoopDevice(loopDev, _const.AppDataMountPath, compression)
	if err != nil {
		log.Panicf("Cannot mount the backing file. %v", err)
	}
	defer func(mountPoint *device.MountPoint) {
		err := device.Unmount(mountPoint)
		if err != nil {
			log.Printf("WARN: cannot unmount the backing file.")
		}
	}(mountPoint)

	if err := utils.ClearTempDirectory(_const.AppTmpPath); err != nil {
		log.Printf("WARN: cannot clean temp directory %s", _const.AppTmpPath)
	}

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
		if err := command.CreateFullBackup(mountPoint, ydbParams, compression); err != nil {
			log.Panicf("Cannot perform full backup: %v", err)
		}
		break
	case cmd.CreateIncrementalBackup:
		ydbParams := initYdbParams()
		if err := command.CreateIncrementalBackup(mountPoint, ydbParams, compression); err != nil {
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
