package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type LoopDevicesObj struct {
	Loopdevices []LoopDevice
}

type LoopDevice struct {
	Name      string
	Sizelimit int
	Autoclear bool
	Ro        bool
	BackFile  string `json:"back-file"`
	Dio       bool
	LogSec    int `json:"log-sec"`
}

const APP_PATH = "/var/lib/ydb-backup-tool/"

func main() {
	dbUrl := os.Getenv("YDB_CONNECTION_URL")
	if dbUrl == "" {
		log.Fatal("YDB_CONNECTION_URL is empty in env")
	}
	dbName := os.Getenv("YDB_DB_NAME")
	if dbName == "" {
		log.Fatal("YDB_DB_NAME is empty in env")
	}

	// Main logic
	//mkdirPath, err := exec.LookPath("mkdir")
	//if err != nil {
	//	log.Fatal("mkdir is not found in %PATH%")
	//}
	//
	//tempFolder := "tmp_backup_" + strconv.Itoa(int(time.Now().Unix()))
	//mkdirCmd := exec.Command(mkdirPath, tempFolder)
	//if err := mkdirCmd.Run(); err != nil {
	//	log.Fatal("Failed to create temporary directory")
	//}

	// TODO: add search is user's profile directory if running as sudo
	ydbPath, err := exec.LookPath("ydb")
	if err != nil {
		log.Fatal("YDB CLI is not found in %PATH%")
	}

	// Perform full backup of YDB
	targetFolder := "ydb_backup_" + strconv.Itoa(int(time.Now().Unix()))
	ydbCmd := exec.Command(ydbPath, "-e", dbUrl, "-d", dbName, "tools", "dump", "-o", targetFolder)
	if err := ydbCmd.Run(); err != nil {
		log.Fatal("Error occurred during YDB backup process: ", err)
	}

	// TODO: think about collisions for db names(different hosts)

	// Verify img file exists or create it in case of absence
	btrfsFileName := strings.ReplaceAll(dbName, "/", "_") + ".img"
	btrfsImgFilePath := APP_PATH + btrfsFileName
	log.Print("btrfsFileName = " + btrfsImgFilePath)
	if _, err = os.Stat(btrfsImgFilePath); err != nil {
		createBtrfsFile(btrfsImgFilePath)
	}

	losetupPath, err := exec.LookPath("losetup")
	if err != nil {
		log.Fatal("`losetup` is not found in %PATH%")
	}

	losetupCmd := exec.Command(losetupPath, "-fP", btrfsImgFilePath)
	if err := losetupCmd.Run(); err != nil {
		log.Fatal("Error occurred during creation of loop device for img file = " + btrfsImgFilePath)
	}

	losetupDevicesCmd := exec.Command(losetupPath, "--json")
	out, err := losetupDevicesCmd.Output()
	if err != nil {
		log.Fatal(err)
	}

	var loopDeviceObj LoopDevicesObj
	json.Unmarshal(out, &loopDeviceObj)

	var btrfsLoopDevice *LoopDevice = nil
	for _, d := range loopDeviceObj.Loopdevices {
		if strings.EqualFold(d.BackFile, btrfsImgFilePath) {
			btrfsLoopDevice = &d
			break
		}
	}
	if btrfsLoopDevice == nil {
		log.Fatal("Coulnd't find loop device")
	}

	//mount / dev / l o o p 0 / mnt / b t r f s
	mountPath, err := exec.LookPath("mount")
	if err != nil {
		log.Fatal("`mount` is not found in %PATH%")
	}

	btrfsMountPoint := APP_PATH + "/mnt"
	createDirectory(btrfsMountPoint)

	mountCmd := exec.Command(mountPath, btrfsLoopDevice.Name, btrfsMountPoint)
	if err := mountCmd.Run(); err != nil {
		log.Fatal("Error during mounting of loopdevice to btrfs folder = " + btrfsMountPoint)
	}

	fmt.Println("success")
}

func createBtrfsFile(fileName string) {
	// Create directory for app data in case it doesn't exist
	createDirectory(APP_PATH)

	ddPath, err := exec.LookPath("dd")
	if err != nil {
		log.Fatal("`dd` is not found in %PATH%")
	}
	ddCmd := exec.Command(ddPath, "if=/dev/zero", "of="+fileName, "bs=1M", "count=1024")
	if err := ddCmd.Run(); err != nil {
		log.Fatal("Failed to create img file: " + fileName)
	}

	mkfsPath, err := exec.LookPath("mkfs.btrfs")
	if err != nil {
		log.Fatal("`mkfs.btrfs` is not found in %PATH%")
	}
	mkfsCmd := exec.Command(mkfsPath, fileName)
	if err := mkfsCmd.Run(); err != nil {
		log.Fatal("Failed to initialize btrfs in the file: " + fileName)
	}
}

func createDirectory(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			log.Fatal("Cannot create base directory for the app: " + dir)
		}
	}
}
