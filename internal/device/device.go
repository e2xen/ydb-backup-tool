package device

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	comp "ydb-backup-tool/internal/btrfs/compression"
	_const "ydb-backup-tool/internal/const"
	"ydb-backup-tool/internal/utils"
)

type BackingFile struct {
	Path string
}

type loopDeviceJson struct {
	Name      string
	Sizelimit int
	Autoclear bool
	Ro        bool
	BackFile  string `json:"back-file"`
	Dio       bool
	LogSec    int `json:"log-sec"`
}

type loopDevicesJson struct {
	Loopdevices []loopDeviceJson
}

type LoopDevice struct {
	Name      string
	Sizelimit int
	Autoclear bool
	Ro        bool
	BackFile  BackingFile
	Dio       bool
	LogSec    int `json:"log-sec"`
}

type MountPoint struct {
	Path    string
	LoopDev LoopDevice
}

func Unmount(mountPoint *MountPoint) error {
	umountPath, err := utils.GetBinary("umount")
	if err != nil {
		return err
	}

	umountCmd := utils.BuildCommand(umountPath, mountPoint.Path)
	if err := umountCmd.Run(); err != nil {
		return fmt.Errorf("cannot unmount the image file")
	}

	return nil
}

func GetOrCreateBackingStoreFile(filePath string) (*BackingFile, bool, error) {
	if _, err := os.Stat(filePath); err != nil {
		if err := createBackingStoreFile(filePath); err != nil {
			return nil, false, err
		}
		return &BackingFile{filePath}, true, nil
	}
	return &BackingFile{filePath}, false, nil
}

func SetupLoopDevice(backingFile *BackingFile) (*LoopDevice, error) {
	losetupPath, err := utils.GetBinary("losetup")
	if err != nil {
		return nil, err
	}

	losetupCmd := utils.BuildCommand(losetupPath, "-fP", backingFile.Path)
	if err := losetupCmd.Run(); err != nil {
		return nil, fmt.Errorf("cannot create loop device with backing file = %s", backingFile.Path)
	}

	losetupDevicesCmd := utils.BuildCommand(losetupPath, "--json")
	out, err := losetupDevicesCmd.Output()
	if err != nil {
		return nil, errors.New("cannot get list of loopback devices")
	}

	var loopDevicesJson loopDevicesJson
	if err := json.Unmarshal(out, &loopDevicesJson); err != nil {
		return nil, errors.New("cannot deserialize json with loopback devices")
	}

	var loopDevJson *loopDeviceJson = nil
	for _, d := range loopDevicesJson.Loopdevices {
		if strings.EqualFold(d.BackFile, backingFile.Path) {
			loopDevJson = &d
			break
		}
	}
	if loopDevJson == nil {
		return nil, errors.New("cannot find loop device")
	}

	return &LoopDevice{
		Name:      loopDevJson.Name,
		Sizelimit: loopDevJson.Sizelimit,
		Autoclear: loopDevJson.Autoclear,
		Ro:        loopDevJson.Autoclear,
		BackFile:  BackingFile{loopDevJson.BackFile},
		Dio:       loopDevJson.Dio,
		LogSec:    loopDevJson.LogSec,
	}, nil
}

func DetachLoopDevice(device *LoopDevice) error {
	losetupPath, err := utils.GetBinary("losetup")
	if err != nil {
		return err
	}

	cmd := utils.BuildCommand(losetupPath, "-d", device.Name)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("cannot detach loop device %s", device.Name)
	}

	return nil
}

func MountLoopDevice(loopDevice *LoopDevice, mountTargetPath string, compression *comp.Compression) (*MountPoint, error) {
	if err := utils.CreateDirectory(mountTargetPath); err != nil {
		return nil, err
	}

	mountPath, err := utils.GetBinary("mount")
	if err != nil {
		return nil, err
	}

	var args []string
	if compression != nil {
		args = append(args, "-o", fmt.Sprintf("compress=%s:%d", (*compression).Algorithm(), (*compression).CompressionLevel()))
	}
	args = append(args, loopDevice.Name, mountTargetPath)

	mountCmd := utils.BuildCommand(mountPath, args...)

	if err := mountCmd.Run(); err != nil {
		return nil, fmt.Errorf("cannot mount loopdevice to folder %s", mountTargetPath)
	}

	return &MountPoint{Path: mountTargetPath, LoopDev: *loopDevice}, nil
}

func ExtendBackingStoreFileBy(backingFile *BackingFile, size int64) error {
	currentSize, err := utils.GetFileSize(backingFile.Path)
	if err != nil {
		return fmt.Errorf("failed to get the file size of %s", backingFile.Path)
	}

	if size < 0 {
		return fmt.Errorf("not allowed to shrink the backing file %s", backingFile.Path)
	}

	if size > 0 {
		// Bytes to MB
		targetSizeInMb := size / (1024 * 1024)
		if size%(1024*1024) != 0 {
			targetSizeInMb += 1
		}
		// Bytes to MB
		targetSizeInMb += currentSize / (1024 * 1024)
		if currentSize%(1024*1024) != 0 {
			targetSizeInMb += 1
		}

		dd, err := utils.GetBinary("dd")
		if err != nil {
			return err
		}

		cmd := utils.BuildCommand(dd, "if=/dev/zero", "bs=1M", fmt.Sprintf("seek=%d", targetSizeInMb),
			"count=0", fmt.Sprintf("of=%s", backingFile.Path))
		fmt.Println(cmd.String())
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to extend backing file %s size to %dMB", backingFile.Path, size)
		}
	}

	return nil
}

func createBackingStoreFile(filePath string) error {
	// Create directory for app data in case it doesn't exist
	if err := utils.CreateDirectory(_const.AppDataPath); err != nil {
		return err
	}

	ddPath, err := utils.GetBinary("dd")
	if err != nil {
		return err
	}
	ddCmd := utils.BuildCommand(ddPath, "if=/dev/zero", "of="+filePath, "bs=1M", "count=256")
	if err := ddCmd.Run(); err != nil {
		return fmt.Errorf("failed to create img file: %s", filePath)
	}

	return nil
}
