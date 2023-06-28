package main

import (
	"flag"
	log "github.com/sirupsen/logrus"
	"strings"
	"ydb-backup-tool/internal/btrfs"
	comp "ydb-backup-tool/internal/btrfs/compression"
	dedup "ydb-backup-tool/internal/btrfs/deduplication/duperemove"
	cmd "ydb-backup-tool/internal/command"
	_const "ydb-backup-tool/internal/const"
	"ydb-backup-tool/internal/device"
	"ydb-backup-tool/internal/utils"
	"ydb-backup-tool/internal/ydb"
)

var (
	ydbEndpoint             *string
	ydbName                 *string
	ydbYcTokenFile          *string
	ydbIamTokenFile         *string
	ydbSaKeyFile            *string
	ydbProfile              *string
	compressionAlgorithm    *string
	compressionLevel        *uint64
	dedupBlockSize          *uint64
	ydbDumpPath             *string
	ydbDumpExclude          *string
	ydbDumpConsistencyLevel *string
	ydbRestorePath          *string
	ydbRestoreData          *uint64
	ydbRestoreIndexes       *uint64
	compression             *comp.Compression
)

func init() {
	ydbEndpoint = flag.String(_const.YdbEndpointArg, "", "YDB endpoint.")
	ydbName = flag.String(_const.YdbNameArg, "", "YDB database name.")
	ydbYcTokenFile = flag.String(_const.YdbYcTokenFileArg, "", "YDB OAuth token file.")
	ydbIamTokenFile = flag.String(_const.YdbIamTokenFileArg, "", "YDB IAM token file.")
	ydbSaKeyFile = flag.String(_const.YdbSaKeyFileArg, "", "YDB Service Account Key file.")
	ydbProfile = flag.String(_const.YdbProfileArg, "", "YDB profile name.")
	compressionAlgorithm = flag.String(_const.CompressionAlgorithmArg, "zstd", "Compression algorithm. Default is ZSTD.")
	compressionLevel = flag.Uint64(_const.CompressionLevelArg, 3, "Compression level. Default is 3.")
	dedupBlockSize = flag.Uint64(_const.DedupBlockSize, 4096, "Block size for reading file extents. Default is 4096 bytes.")
	ydbDumpPath = flag.String(_const.YdbDumpPath, ".", "Path to the database directory with objects or a path to the table to be dumped.The root database directory is used by default.")
	ydbDumpExclude = flag.String(_const.YdbDumpExclude, "", "Template (PCRE) to exclude paths from export.")
	ydbDumpConsistencyLevel = flag.String(_const.YdbDumpConsistencyLevel, "database", "The consistency level. Possible options: database and table. Default is database.")
	ydbRestorePath = flag.String(_const.YdbRestorePath, ".", "Path to the database directory the data will be imported to. Default is the root directory.")
	ydbRestoreData = flag.Uint64(_const.YdbRestoreData, 1, "Enables/disables data import, 1 (yes) or 0 (no), defaults to 1.")
	ydbRestoreIndexes = flag.Uint64(_const.YdbRestoreIndexes, 1, "Enables/disables import of indexes, 1 (yes) or 0 (no), defaults to 1.")

	flag.Bool(_const.YdbUseMetadataCredsArg, false, "YDB use the metadata service.")
	flag.Bool(_const.YdbDumpSchemeOnly, false, "Dump only the details about the database schema objects, without dumping their data.")
	flag.Bool(_const.YdbDumpAvoidCopy, false, "Do not create a snapshot before dumping.")
	flag.Bool(_const.YdbRestoreDryRun, false, "Matching the data schemas in the database and file system without updating the database, 1 (yes) or 0 (no), defaults to 0.")

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
	case "lss", "list-sizes":
		command = cmd.ListAllBackupsSizes
	case "ls", "list":
		command = cmd.ListAllBackups
		break
	case "cr", "create":
		command = cmd.CreateIncrementalBackup
		break
	case "rs", "restore":
		command = cmd.RestoreFromBackup
		break
	default:
		log.Panicf("Could not parse command")
	}

	return &command
}

func main() {
	command := parseAndValidateArgs()

	// TODO: add "--help" option

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
			log.Warnf("cannot detach the loop device.")
		}
	}(loopDev)

	mountPoint, err := device.MountLoopDevice(loopDev, _const.AppDataMountPath, compression)
	if err != nil {
		log.Panicf("Cannot mount the backing file. %v", err)
	}
	defer func(mountPoint *device.MountPoint) {
		err := device.Unmount(mountPoint)
		if err != nil {
			log.Warnf("cannot unmount the backing file.")
		}
	}(mountPoint)

	if err := utils.ClearTempDirectory(_const.AppTmpPath); err != nil {
		log.Warnf("cannot clean temp directory %s", _const.AppTmpPath)
	}

	switch *command {
	case cmd.ListAllBackups:
		err := command.ListBackups(mountPoint)
		if err != nil {
			log.Panicf("Cannot list backups: %v", err)
		}
		break
	case cmd.ListAllBackupsSizes:
		err := command.ListBackupsSizes(mountPoint)
		if err != nil {
			log.Panicf("Cannot list backup sizes: %v", err)
		}
	case cmd.CreateIncrementalBackup:
		ydbParams := initYdbParams()
		dedupParams := &dedup.Params{BlockSize: *dedupBlockSize}
		ydbDumpParams := &ydb.DumpParams{
			Path:             *ydbDumpPath,
			Exclude:          *ydbDumpExclude,
			ConsistencyLevel: *ydbDumpConsistencyLevel,
			AvoidCopy:        isArgFlagPassed(_const.YdbDumpAvoidCopy),
			SchemeOnly:       isArgFlagPassed(_const.YdbDumpSchemeOnly),
		}
		if err := command.CreateIncrementalBackup(mountPoint, ydbParams, ydbDumpParams, compression, dedupParams); err != nil {
			log.Panicf("Cannot perform incremental backup: %v", err)
		}
		break
	case cmd.RestoreFromBackup:
		if len(flag.Args()) <= 1 {
			log.Panic("You should specify backup name: restore <name>")
		}

		sourcePath := flag.Arg(1)
		ydbParams := initYdbParams()
		restoreParams := &ydb.RestoreParams{
			Path:    *ydbRestorePath,
			Data:    *ydbRestoreData,
			Indexes: *ydbRestoreIndexes,
			DryRun:  isArgFlagPassed(_const.YdbRestoreDryRun),
		}
		if err := command.RestoreFromBackup(mountPoint, ydbParams, restoreParams, sourcePath); err != nil {
			log.Panicf("Cannot restore from the backup: %v", err)
		}
		break
	}
}

func initYdbParams() *ydb.YdbParams {
	return &ydb.YdbParams{Endpoint: *ydbEndpoint,
		Name:             *ydbName,
		YcTokenFile:      *ydbYcTokenFile,
		IamTokenFile:     *ydbIamTokenFile,
		SaKeyFile:        *ydbSaKeyFile,
		Profile:          *ydbProfile,
		UseMetadataCreds: isArgFlagPassed(_const.YdbUseMetadataCredsArg),
	}
}
