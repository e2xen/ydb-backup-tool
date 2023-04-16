package main

import (
	"flag"
	"log"
	"strings"
	"ydb-backup-tool/internal/btrfs"
	cmd "ydb-backup-tool/internal/command"
	_const "ydb-backup-tool/internal/const"
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
	//dbUrl := os.Getenv("YDB_CONNECTION_URL")
	//if dbUrl == "" {
	//	log.Fatal("YDB_CONNECTION_URL is empty in env")
	//}
	//dbName := os.Getenv("YDB_DB_NAME")
	//if dbName == "" {
	//	log.Fatal("YDB_DB_NAME is empty in env")
	//}
	flag.Parse()

	if strings.TrimSpace(*ydbEndpoint) == "" {
		log.Fatal("You need to specify YDB url passing the following parameter: \"--ydb-endpoint=<url>\"")
	}
	if strings.TrimSpace(*ydbName) == "" {
		log.Fatal("You need to specify YDB database name passing the following parameter: \"--ydb-name=<name>\"")
	}
	if len(flag.Args()) == 0 {
		log.Fatal("You need to pass command")
	}

	var command cmd.Command
	switch strings.TrimSpace(flag.Arg(0)) {
	case "ls":
		command = cmd.ListAllBackups
		break
	case "create-full":
		command = cmd.CreateFullBackup
		break
	case "create-inc":
		command = cmd.CreateIncrementalBackup
		break
	default:
		command = cmd.Undefined
	}

	if command == cmd.Undefined {
		log.Fatal("Couldn't parse command")
	}

	return &command
}

func main() {
	command := parseAndValidateArgs()

	// TODO: add an optional param to name user's backups

	// TODO: check if running as sudo

	// TODO: add "--help" option
	// TODO: add "--debug" option

	// TODO: think about collisions for db names(different hosts) OR create only one .img file for all backups(for example, data.img)
	btrfsFileName := strings.ReplaceAll(*ydbName, "/", "_") + ".img"
	// Verify img file exists or create it in case of absence
	btrfsImgFile, err := btrfs.GetOrCreateBtrfsImgFile(btrfsFileName)
	if err != nil {
		log.Fatal("Cannot obtain btrfs image file. ", err)
	}
	btrfsMountPoint, err := btrfs.MountImgFile(btrfsImgFile)
	if err != nil {
		log.Fatal("Cannot mount btrfs image file. ", err)
	}
	// TODO: there was a bug - it was not called sometimes in case of failure (after log.Fatal())
	defer func(mountPoint *btrfs.MountPoint) {
		err := btrfs.Unmount(mountPoint)
		if err != nil {
			log.Printf("WARN: cannot unmount the image file.")
		}
	}(btrfsMountPoint)

	switch *command {
	case cmd.ListAllBackups:
		err := command.ListBackups(btrfsMountPoint)
		if err != nil {
			log.Printf("cannot list backups because of the following error: %s", err)
			return
		}
		break
	case cmd.CreateFullBackup:
		ydbParams := initYdbParams()
		err := command.CreateFullBackup(btrfsMountPoint, ydbParams)
		if err != nil {
			log.Printf("cannot perform full backup because of the following error: %s", err)
			return
		}
		break
	case cmd.CreateIncrementalBackup:
		if len(flag.Args()) <= 1 {
			if err := btrfs.Unmount(btrfsMountPoint); err != nil {
				log.Printf("WARN: cannot unmount the image file.")
			}
			log.Printf("You should specify base backup: create-inc <base_backup>")
			return
		}

		baseBackup := flag.Arg(1)
		ydbParams := initYdbParams()
		err := command.CreateIncrementalBackup(btrfsMountPoint, ydbParams, baseBackup)
		if err != nil {
			log.Printf("cannot perform incremental backup because of the following error: %s", err)
			return
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
