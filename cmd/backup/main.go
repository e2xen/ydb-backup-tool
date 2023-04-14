package main

import (
	"flag"
	"log"
	"strings"
	"ydb-backup-tool/internal/btrfs"
	cmd "ydb-backup-tool/internal/command"
	"ydb-backup-tool/internal/dump"
)

var (
	ydbUrl  *string
	ydbName *string
)

func init() {
	ydbUrl = flag.String("ydb-url", "", "YDB url")
	ydbName = flag.String("ydb-name", "", "YDB name")
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

	if strings.TrimSpace(*ydbUrl) == "" {
		log.Fatal("You need to specify YDB url passing the following parameter: \"--ydb-url=<url>\"")
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

	// TODO: check if running as sudo

	// TODO: think about collisions for db names(different hosts)
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
	defer func(mountPoint *btrfs.MountPoint) {
		err := btrfs.UnmountImgFile(mountPoint)
		if err != nil {
			log.Printf("WARN: cannot unmount the image file.")
		}
	}(btrfsMountPoint)

	switch *command {
	case cmd.ListAllBackups:
		err := command.ListBackups(btrfsMountPoint)
		if err != nil {
			log.Fatal("cannot list backups because of the following error: ", err)
		}
		break
	case cmd.CreateFullBackup:
		ydbParams := dump.YdbParams{DbUrl: ydbUrl, DbName: ydbName}
		err := command.CreateFullBackup(btrfsMountPoint, &ydbParams)
		if err != nil {
			log.Fatal("cannot perform full backup because of the following error: ", err)
		}
		break
	case cmd.CreateIncrementalBackup:
		if len(flag.Args()) <= 1 {
			if err := btrfs.UnmountImgFile(btrfsMountPoint); err != nil {
				log.Printf("WARN: cannot unmount the image file.")
			}
			log.Fatal("You should specify base backup: create-inc <base_backup>")
		}

		baseBackup := flag.Arg(1)
		ydbParams := dump.YdbParams{DbUrl: ydbUrl, DbName: ydbName}
		err := command.CreateIncrementalBackup(btrfsMountPoint, &ydbParams, &baseBackup)
		if err != nil {
			log.Fatal("cannot perform incremental backup because of the following error: ", err)
		}
		break
	}
}
