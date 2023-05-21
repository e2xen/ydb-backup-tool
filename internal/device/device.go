package device

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
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

type BackingFile struct {
	Path string
}

type MountPoint struct {
	Path string
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

func createBackingStoreFile(filePath string) error {
	// Create directory for app data in case it doesn't exist
	if err := utils.CreateDirectory(_const.AppDataPath); err != nil {
		return err
	}

	ddPath, err := utils.GetBinary("dd")
	if err != nil {
		return err
	}
	ddCmd := utils.BuildCommand(ddPath, "if=/dev/zero", "of="+filePath, "bs=1M", "count=1024")
	if err := ddCmd.Run(); err != nil {
		return fmt.Errorf("failed to create img file: %s", filePath)
	}

	return nil
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

	var loopDeviceObj LoopDevicesObj
	if err := json.Unmarshal(out, &loopDeviceObj); err != nil {
		return nil, errors.New("cannot deserialize json with loopback devices")
	}

	var loopDevice *LoopDevice = nil
	for _, d := range loopDeviceObj.Loopdevices {
		if strings.EqualFold(d.BackFile, backingFile.Path) {
			loopDevice = &d
			break
		}
	}
	if loopDevice == nil {
		return nil, errors.New("cannot find loop device")
	}

	return loopDevice, nil
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

func MountLoopDevice(loopDevice *LoopDevice, mountTargetPath string) (*MountPoint, error) {
	if err := utils.CreateDirectory(mountTargetPath); err != nil {
		return nil, err
	}

	mountPath, err := utils.GetBinary("mount")
	if err != nil {
		return nil, err
	}

	mountCmd := utils.BuildCommand(mountPath, loopDevice.Name, mountTargetPath)

	if err := mountCmd.Run(); err != nil {
		return nil, fmt.Errorf("cannot mount loopdevice to folder %s", mountTargetPath)
	}

	return &MountPoint{mountTargetPath}, nil
}
