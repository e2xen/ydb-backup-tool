package btrfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	_const "ydb-backup-tool/internal/const"
	"ydb-backup-tool/internal/utils"
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

type ImgFile struct {
	Path string
}

type MountPoint struct {
	Path string
}

type Subvolume struct {
	Path string
}

type Snapshot struct {
	Path string
}

func GetOrCreateBtrfsImgFile(btrfsFileName string) (*ImgFile, error) {
	btrfsImgFilePath := _const.AppPath + btrfsFileName
	log.Print("btrfsFileName = " + btrfsImgFilePath)
	if _, err := os.Stat(btrfsImgFilePath); err != nil {
		if err := createBtrfsFile(btrfsImgFilePath); err != nil {
			return nil, err
		}
	}
	return &ImgFile{btrfsImgFilePath}, nil
}

func MountImgFile(imgFile *ImgFile) (*MountPoint, error) {
	// TODO: unmount device (in case it already exists)
	// TODO: losetup in case device is added

	losetupPath, err := exec.LookPath("losetup")
	if err != nil {
		return nil, fmt.Errorf("`losetup` is not found in PATH. Error: %w", err)
	}

	// TODO: check it already exists
	losetupCmd := exec.Command(losetupPath, "-fP", imgFile.Path)
	if err := losetupCmd.Run(); err != nil {
		return nil, fmt.Errorf("cannot create loop device with img file = %s. Error: %w", imgFile.Path, err)
	}

	losetupDevicesCmd := exec.Command(losetupPath, "--json")
	out, err := losetupDevicesCmd.Output()
	if err != nil {
		return nil, fmt.Errorf("cannot get list of loopback devices. Error: %w", err)
	}

	var loopDeviceObj LoopDevicesObj
	if err := json.Unmarshal(out, &loopDeviceObj); err != nil {
		return nil, fmt.Errorf("cannot deserialize json with loopback devices. Error: %w", err)
	}

	var btrfsLoopDevice *LoopDevice = nil
	for _, d := range loopDeviceObj.Loopdevices {
		if strings.EqualFold(d.BackFile, imgFile.Path) {
			btrfsLoopDevice = &d
			break
		}
	}
	if btrfsLoopDevice == nil {
		return nil, errors.New("cannot find loop device")
	}

	btrfsMountPoint := _const.AppMountPath
	if err := utils.CreateDirectory(btrfsMountPoint); err != nil {
		return nil, err
	}

	mountPath, err := exec.LookPath("mount")
	if err != nil {
		return nil, fmt.Errorf("`mount` is not found in PATH. Error: %w", err)
	}

	// TODO: umount in case the loop device was already mounted

	mountCmd := exec.Command(mountPath, btrfsLoopDevice.Name, btrfsMountPoint)
	if err := mountCmd.Run(); err != nil {
		return nil, fmt.Errorf("cannot mount loopdevice to btrfs folder = %s. Error: %w", btrfsMountPoint, err)
	}

	return &MountPoint{btrfsMountPoint}, nil
}

/* It will not work with recursive subvolumes */

func CreateSubvolume(path *string) (*Subvolume, error) {
	btrfsPath, err := exec.LookPath("btrfs")
	if err != nil {
		return nil, fmt.Errorf("`mount` is not found in PATH. Error: %w", err)
	}

	btrfsCmd := exec.Command(btrfsPath, "subvolume", "create", *path)
	if err := btrfsCmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to create subvolume: %s. Error: %w", *path, err)
	}

	return &Subvolume{*path}, nil
}

func CreateSnapshot(subvolume *Subvolume, snapshotTargetPath *string) (*Snapshot, error) {
	subvolumeExists, err := verifySubvolumeExists(subvolume)

	if err != nil {
		return nil, fmt.Errorf("cannot verify that subvolume exists. Error: %w", err)
	}
	if !subvolumeExists {
		return nil, fmt.Errorf("cannot find subvolume: %s", err)
	}

	btrfsPath, err := exec.LookPath("btrfs")
	if err != nil {
		return nil, fmt.Errorf("`mount` is not found in PATH. Error: %w", err)
	}

	btrfsCmd := exec.Command(btrfsPath, "subvolume", "snapshot", "-r", subvolume.Path, *snapshotTargetPath)
	if err := btrfsCmd.Run(); err != nil {
		return nil, fmt.Errorf("cannot create snapshot %s. Error: %w", *snapshotTargetPath, err)
	}

	return &Snapshot{*snapshotTargetPath}, nil
}

func GetSubvolumes(path *string) ([]*Subvolume, error) {
	btrfsPath, err := exec.LookPath("btrfs")
	if err != nil {
		return nil, fmt.Errorf("`mount` is not found in PATH. Error: %w", err)
	}

	btrfsCmd := exec.Command(btrfsPath, "subvolume", "list", *path)
	out, err := btrfsCmd.Output()
	if err != nil {
		return nil, fmt.Errorf("cannot get list of subvolumes. Error: %w", err)
	}

	result := []*Subvolume{}
	for _, subvolume := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(subvolume) != "" {
			words := strings.Split(subvolume, " ")
			path := *path + "/" + words[len(words)-1]
			result = append(result, &Subvolume{path})
		}
	}
	return result, nil
}

func GetSnapshots(path *string) ([]*Snapshot, error) {
	btrfsPath, err := exec.LookPath("btrfs")
	if err != nil {
		return nil, fmt.Errorf("`mount` is not found in PATH. Error: %w", err)
	}

	btrfsCmd := exec.Command(btrfsPath, "subvolume", "list", "-r", *path)
	out, err := btrfsCmd.Output()
	if err != nil {
		return nil, fmt.Errorf("cannot get list of snapshots. Error: %w", err)
	}

	result := []*Snapshot{}
	for _, subvolume := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(subvolume) != "" {
			words := strings.Split(subvolume, " ")
			path := *path + "/" + words[len(words)-1]
			result = append(result, &Snapshot{path})
		}
	}
	return result, nil
}

func GetSnapshot(path *string) (*Snapshot, error) {
	dir := filepath.Dir(*path)
	snapshots, err := GetSnapshots(&dir)
	if err != nil {
		return nil, fmt.Errorf("cannot get list of snapshots. Error: %w", err)
	}

	for _, snapshot := range snapshots {
		if snapshot.Path == *path {
			return snapshot, nil
		}
	}

	return nil, nil
}

func GetSubvolume(path *string) (*Subvolume, error) {
	// Extract dir
	dir := filepath.Dir(*path)

	subvolumes, err := GetSubvolumes(&dir)
	if err != nil {
		return nil, fmt.Errorf("cannot get list of subvolumes. Error: %w", err)
	}

	for _, subvolume := range subvolumes {
		if subvolume.Path == *path {
			return subvolume, nil
		}
	}

	return nil, nil
}

func DeleteSubvolume(subvolume *Subvolume) error {
	btrfsPath, err := exec.LookPath("btrfs")
	if err != nil {
		return fmt.Errorf("`mount` is not found in PATH. Error: %w", err)
	}

	subvolumeExists, err := verifySubvolumeExists(subvolume)
	if err != nil {
		return fmt.Errorf("failed to verify the existence of the following subvolume: %s. Error: %w", subvolume.Path, err)
	}
	if !subvolumeExists {
		return fmt.Errorf("subvolume %s does not exist", subvolume.Path)
	}

	btrfsCmd := exec.Command(btrfsPath, "subvolume", "delete", subvolume.Path)
	if err := btrfsCmd.Run(); err != nil {
		return fmt.Errorf("failed to delete the following subvolume: %s. Error: %w", subvolume.Path, err)
	}

	return nil
}

func DeleteSnapshot(snapshot *Snapshot) error {
	return DeleteSubvolume(&Subvolume{snapshot.Path})
}

func UnmountImgFile(mountPoint *MountPoint) error {
	umountPath, err := exec.LookPath("umount")
	if err != nil {
		return fmt.Errorf("`umount` is not found in PATH. Error: %w", err)
	}

	umountCmd := exec.Command(umountPath, mountPoint.Path)
	if err := umountCmd.Run(); err != nil {
		return fmt.Errorf("cannot unmount the image file. Error: %w", err)
	}

	return nil
}

func CreateIncrementalSnapshot(prevSnapshot *Snapshot, newSnapshot *Snapshot, target *Subvolume) (*Snapshot, error) {
	//sudo btrfs send -p /.snapshot/home-day2 /.snapshot/home-day3 | sudo btrfs receive /run/media/user/mydisk/bk
	btrfsPath, err := exec.LookPath("btrfs")
	if err != nil {
		return nil, fmt.Errorf("`mount` is not found in PATH. Error: %w", err)
	}

	tempIncDiffFile := newSnapshot.Path + "_temp_diff"
	if err := utils.CreateFile(&tempIncDiffFile); err != nil {
		return nil, fmt.Errorf("failed to create temporary file %s to store diff", tempIncDiffFile)
	}

	btrfsCmdSend := exec.Command(btrfsPath, "send", "-p", prevSnapshot.Path, newSnapshot.Path, "-f", tempIncDiffFile)
	if err := btrfsCmdSend.Run(); err != nil {
		return nil, fmt.Errorf("cannot create incremental snapshot given %s and %s", prevSnapshot.Path, newSnapshot.Path)
	}

	// Delete new snapshot
	if err := DeleteSnapshot(newSnapshot); err != nil {
		return nil, fmt.Errorf("failed to delete snapshot %s", newSnapshot.Path)
	}

	btrfsCmdReceive := exec.Command(btrfsPath, "receive", "-f", tempIncDiffFile, target.Path)
	if err := btrfsCmdReceive.Run(); err != nil {
		return nil, fmt.Errorf("cannot create incremental snapshot from the diff file %s", tempIncDiffFile)
	}

	// Delete file with temp diff
	if err := utils.DeleteFile(&tempIncDiffFile); err != nil {
		return nil, fmt.Errorf("failed to delete file %s with incremental snapshot difference", tempIncDiffFile)
	}

	return &Snapshot{newSnapshot.Path}, nil
}

func createBtrfsFile(fileName string) error {
	// Create directory for app data in case it doesn't exist
	if err := utils.CreateDirectory(_const.AppPath); err != nil {
		return err
	}

	ddPath, err := exec.LookPath("dd")
	if err != nil {
		return errors.New("`dd` is not found in %PATH%")
	}
	ddCmd := exec.Command(ddPath, "if=/dev/zero", "of="+fileName, "bs=1M", "count=1024")
	if err := ddCmd.Run(); err != nil {
		return fmt.Errorf("failed to create img file: %s. Error: %w", fileName, err)
	}

	mkfsPath, err := exec.LookPath("mkfs.btrfs")
	if err != nil {
		return errors.New("`mkfs.btrfs` is not found in %PATH%")
	}
	mkfsCmd := exec.Command(mkfsPath, fileName)
	if err := mkfsCmd.Run(); err != nil {
		return fmt.Errorf("failed to initialize btrfs in the file: %s. Error: %w", fileName, err)
	}

	return nil
}

func verifySubvolumeExists(subvolume *Subvolume) (bool, error) {
	dir := filepath.Dir(subvolume.Path)

	subvolumes, err := GetSubvolumes(&dir)
	if err != nil {
		return false, fmt.Errorf("cannot get list of subvolumes. Error: %w", err)
	}

	for _, curSubvolume := range subvolumes {
		if curSubvolume.Path == subvolume.Path {
			return true, nil
		}
	}

	return false, nil
}
