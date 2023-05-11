package btrfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/exp/slices"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
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
	Path       string
	Name       string
	IsSnapshot bool
}

type SubvolumeMeta struct {
	Base           Subvolume
	Id             uint64
	CreatedAt      time.Time
	SizeExclusive  uint64
	SizeReferenced uint64
}

func NewSubvolume(path string, isSnapshot bool) *Subvolume {
	pathSplit := strings.Split(path, "/")
	return &Subvolume{Path: path, Name: pathSplit[len(pathSplit)-1], IsSnapshot: isSnapshot}
}

func NewSnapshot(path string) *Subvolume {
	return NewSubvolume(path, true)
}

func GetOrCreateBtrfsImgFile(btrfsFileName string) (*ImgFile, error) {
	if !strings.HasPrefix(btrfsFileName, "/") {
		btrfsFileName = "/" + btrfsFileName
	}

	btrfsImgFilePath := _const.AppDataPath + btrfsFileName
	if _, err := os.Stat(btrfsImgFilePath); err != nil {
		if err := createBtrfsFile(btrfsImgFilePath); err != nil {
			return nil, err
		}
	}
	return &ImgFile{btrfsImgFilePath}, nil
}

func SetupLoopDevice(imgFile *ImgFile) (*LoopDevice, error) {
	// TODO: unmount device (in case it already exists)

	losetupPath, err := utils.GetBinary("losetup")
	if err != nil {
		return nil, err
	}

	// TODO: check it already exists (or detach)
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

	return btrfsLoopDevice, nil
}

func DetachLoopDevice(device *LoopDevice) error {
	losetupPath, err := utils.GetBinary("losetup")
	if err != nil {
		return err
	}

	cmd := exec.Command(losetupPath, "-d", device.Name)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("cannot detach loop device %s", device.Name)
	}

	return nil
}

func MountLoopDevice(loopDevice *LoopDevice) (*MountPoint, error) {
	btrfsMountPoint := _const.AppDataMountPath
	if err := utils.CreateDirectory(btrfsMountPoint); err != nil {
		return nil, err
	}

	mountPath, err := utils.GetBinary("mount")
	if err != nil {
		return nil, err
	}

	// TODO: umount in case the loop device was already mounted

	mountCmd := exec.Command(mountPath, loopDevice.Name, btrfsMountPoint)
	if err := mountCmd.Run(); err != nil {
		return nil, fmt.Errorf("cannot mount loopdevice to btrfs folder = %s. Error: %w", btrfsMountPoint, err)
	}

	return &MountPoint{btrfsMountPoint}, nil
}

/* It will not work with recursive subvolumes */
func CreateSubvolume(path string) (*Subvolume, error) {
	btrfsPath, err := utils.GetBinary("btrfs")
	if err != nil {
		return nil, err
	}

	btrfsCmd := exec.Command(btrfsPath, "subvolume", "create", path)
	if err := btrfsCmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to create subvolume: %s. Error: %w", path, err)
	}

	return NewSubvolume(path, false), nil
}

func CreateSnapshot(subvolume *Subvolume, snapshotTargetPath string) (*Subvolume, error) {
	subvolumeExists, err := verifySubvolumeExists(subvolume)

	if err != nil {
		return nil, fmt.Errorf("cannot verify that subvolume exists. Error: %w", err)
	}
	if !subvolumeExists {
		return nil, fmt.Errorf("cannot find subvolume: %s", err)
	}

	btrfsPath, err := utils.GetBinary("btrfs")
	if err != nil {
		return nil, err
	}

	btrfsCmd := exec.Command(btrfsPath, "subvolume", "snapshot", "-r", subvolume.Path, snapshotTargetPath)
	if err := btrfsCmd.Run(); err != nil {
		return nil, fmt.Errorf("cannot create snapshot %s. Error: %w", snapshotTargetPath, err)
	}

	return NewSnapshot(snapshotTargetPath), nil
}

/*
* Returns the list of subvolumes (including snapshots)
 */
func GetSubvolumes(path string) ([]*Subvolume, error) {
	btrfsPath, err := utils.GetBinary("btrfs")
	if err != nil {
		return nil, err
	}

	// Firstly, get snapshots
	result, err := GetSnapshots(path)
	if err != nil {
		return nil, err
	}
	btrfsCmd := exec.Command(btrfsPath, "subvolume", "list", path)
	out, err := btrfsCmd.Output()
	if err != nil {
		return nil, fmt.Errorf("cannot get list of subvolumes. Error: %w", err)
	}

	for _, subvolume := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(subvolume) != "" {
			words := strings.Split(subvolume, " ")
			path := path + "/" + words[len(words)-1]

			// If the subvolume is already in the list, then it is a snapshot - ignore it in the loop
			if !slices.Contains(result, NewSnapshot(path)) {
				result = append(result, NewSubvolume(path, false))
			}
		}
	}
	return result, nil
}

func GetSnapshots(path string) ([]*Subvolume, error) {
	btrfsPath, err := utils.GetBinary("btrfs")
	if err != nil {
		return nil, err
	}

	btrfsCmd := exec.Command(btrfsPath, "subvolume", "list", "-r", path)
	out, err := btrfsCmd.Output()
	if err != nil {
		return nil, fmt.Errorf("cannot get list of snapshots. Error: %w", err)
	}

	result := []*Subvolume{}
	for _, subvolume := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(subvolume) != "" {
			words := strings.Split(subvolume, " ")
			path := path + "/" + words[len(words)-1]
			result = append(result, NewSnapshot(path))
		}
	}
	return result, nil
}

func GetSnapshot(path string) (*Subvolume, error) {
	dir := filepath.Dir(path)
	snapshots, err := GetSnapshots(dir)
	if err != nil {
		return nil, fmt.Errorf("cannot get list of snapshots. Error: %w", err)
	}

	for _, snapshot := range snapshots {
		if snapshot.Path == path {
			return snapshot, nil
		}
	}

	return nil, nil
}

func GetSubvolume(path string) (*Subvolume, error) {
	// Extract dir
	dir := filepath.Dir(path)

	subvolumes, err := GetSubvolumes(dir)
	if err != nil {
		return nil, fmt.Errorf("cannot get list of subvolumes. Error: %w", err)
	}

	for _, subvolume := range subvolumes {
		if subvolume.Path == path {
			return subvolume, nil
		}
	}

	return nil, nil
}

func DeleteSubvolume(subvolume *Subvolume) error {
	btrfsPath, err := utils.GetBinary("btrfs")
	if err != nil {
		return err
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

func GetSnapshotsMeta(path string) (*[]SubvolumeMeta, error) {
	snapshots, err := GetSnapshots(path)
	if err != nil {
		return nil, err
	}

	if err := quotaGroupEnable(path); err != nil {
		return nil, fmt.Errorf("failed to enable quota group for the given path: %s", path)
	}

	btrfsPath, err := utils.GetBinary("btrfs")
	if err != nil {
		return nil, err
	}

	var result []SubvolumeMeta

	for _, snapshot := range snapshots {
		cmd := exec.Command(btrfsPath, "subvolume", "show", "-b", snapshot.Path)
		out, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("failed to get meta information about the following snapshot: %s", snapshot.Path)
		}

		subvolumeMeta, err := extractSubvolumeMetaInfo(string(out), NewSnapshot(snapshot.Path))
		if err != nil {
			return nil, err
		}
		result = append(result, *subvolumeMeta)
	}

	return &result, nil
}

func DeleteSnapshot(subvolume *Subvolume) error {
	if !subvolume.IsSnapshot {
		panic("cannot delete snapshot, since subvolume provided")
	}

	return DeleteSubvolume(subvolume)
}

func Unmount(mountPoint *MountPoint) error {
	umountPath, err := utils.GetBinary("umount")
	if err != nil {
		return err
	}

	umountCmd := exec.Command(umountPath, mountPoint.Path)
	if err := umountCmd.Run(); err != nil {
		return fmt.Errorf("cannot unmount the image file. Error: %w", err)
	}

	return nil
}

func CreateIncrementalSnapshot(prevSnapshot *Subvolume, newSnapshot *Subvolume, target *Subvolume) (*Subvolume, error) {
	if !prevSnapshot.IsSnapshot {
		panic("wrong argument provided, since prevSnapshot is not a snapshot")
	}
	if !newSnapshot.IsSnapshot {
		panic("wrong argument provided, since newSnapshot is not a snapshot")
	}

	btrfsPath, err := utils.GetBinary("btrfs")
	if err != nil {
		return nil, err
	}

	tempIncDiffFile := newSnapshot.Path + "_temp_diff"
	if err := utils.CreateFile(tempIncDiffFile); err != nil {
		return nil, fmt.Errorf("failed to create temporary file %s to store diff", tempIncDiffFile)
	}
	defer func() {
		// Delete file with temp diff
		if err := utils.DeleteFile(tempIncDiffFile); err != nil {
			log.Printf("WARIN: failed to delete file %s with incremental snapshot difference", tempIncDiffFile)
		}
	}()

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

	return NewSnapshot(newSnapshot.Path), nil
}

func createBtrfsFile(fileName string) error {
	// Create directory for app data in case it doesn't exist
	if err := utils.CreateDirectory(_const.AppDataPath); err != nil {
		return err
	}

	ddPath, err := utils.GetBinary("dd")
	if err != nil {
		return err
	}
	ddCmd := exec.Command(ddPath, "if=/dev/zero", "of="+fileName, "bs=1M", "count=1024")
	if err := ddCmd.Run(); err != nil {
		return fmt.Errorf("failed to create img file: %s. Error: %w", fileName, err)
	}

	mkfsPath, err := utils.GetBinary("mkfs.btrfs")
	if err != nil {
		return err
	}
	mkfsCmd := exec.Command(mkfsPath, fileName)
	if err := mkfsCmd.Run(); err != nil {
		return fmt.Errorf("failed to initialize btrfs in the file: %s. Error: %w", fileName, err)
	}

	return nil
}

func verifySubvolumeExists(subvolume *Subvolume) (bool, error) {
	dir := filepath.Dir(subvolume.Path)

	subvolumes, err := GetSubvolumes(dir)
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

func quotaGroupEnable(path string) error {
	btrfsPath, err := utils.GetBinary("btrfs")
	if err != nil {
		return err
	}

	btrfsCmd := exec.Command(btrfsPath, "quota", "enable", path)
	if err := btrfsCmd.Run(); err != nil {
		return fmt.Errorf("failed to enable quotas for the path %s", path)
	}

	return nil
}

func extractSubvolumeMetaInfo(text string, baseSubvolume *Subvolume) (*SubvolumeMeta, error) {
	metaMap := make(map[string]string)

	for _, line := range strings.Split(text, "\n") {
		cuttingByDelimiter := strings.SplitN(line, ":", 2)

		if len(cuttingByDelimiter) == 2 {
			key := strings.ToLower(strings.TrimSpace(cuttingByDelimiter[0]))
			value := strings.TrimSpace(cuttingByDelimiter[1])
			metaMap[key] = value
		}
	}

	id, err := strconv.ParseUint(metaMap["subvolume id"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse parameter `subvolume id` for the following subvolume %s",
			baseSubvolume.Path)
	}
	createdAt, err := time.Parse("2006-01-02 15:04:05 -0700", metaMap["creation time"])
	if err != nil {
		return nil, fmt.Errorf("failed to parse parameter `creation time` for the following subvolume %s",
			baseSubvolume.Path)
	}
	sizeExclusive, err := strconv.ParseUint(metaMap["usage exclusive"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse parameter `usage exclusive` for the following subvolume %s",
			baseSubvolume.Path)
	}
	sizeReferenced, err := strconv.ParseUint(metaMap["usage referenced"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse parameter `usage referenced` for the following subvolume %s",
			baseSubvolume.Path)
	}

	return &SubvolumeMeta{Base: *baseSubvolume, Id: id, CreatedAt: createdAt, SizeExclusive: sizeExclusive,
		SizeReferenced: sizeReferenced}, nil
}
