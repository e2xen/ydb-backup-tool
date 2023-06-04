package utils

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
)

func CreateDirectory(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return fmt.Errorf("cannot create the following directory: %s", dir)
		}
	}
	return nil
}

func ClearTempDirectory(path string) error {
	if err := DeleteDirectory(path); err != nil {
		return err
	}
	if err := CreateDirectory(path); err != nil {
		return err
	}
	return nil
}

func GetFileSize(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func GetDirectorySize(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if !fi.IsDir() {
		return 0, fmt.Errorf("%s is not a directory", path)
	}

	var totalSize int64
	err = filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		totalSize += info.Size()
		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("failed to calculate directory size %s due to %s", path, err)
	}

	return totalSize, err
}

func CreateFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file %s", path)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Warnf("cannot close file descriptor of the file %s", path)
		}
	}(f)
	return nil
}

func DeleteFile(path string) error {
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("failed to delete file %s", path)
		}
	}
	return nil
}

func MoveFilesFromDirToDir(source string, target string) error {
	entries, err := os.ReadDir(source)
	if err != nil {
		return fmt.Errorf("failed to get entries from source dir: %s", entries)
	}

	for _, entry := range entries {
		oldPath := path.Join(source, entry.Name())
		newPath := path.Join(target, entry.Name())
		if err := MoveFile(oldPath, newPath); err != nil {
			log.Printf("Error: %s", err)
			return fmt.Errorf("failed to move entry from %s to %s", oldPath, newPath)
		}
	}

	return nil
}

func MoveFile(source string, target string) error {
	if _, err := os.Stat(source); os.IsNotExist(err) {
		return fmt.Errorf("failed to move as %s does not exist", source)
	}

	mvPath, err := GetBinary("mv")
	if err != nil {
		return err
	}

	cmd := BuildCommand(mvPath, source, target)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to move file from %s to %s", source, target)
	}

	return nil
}

func DeleteDirectory(path string) error {
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("failed to delete directory %s", path)
		}
	}
	return nil
}

func GetBinary(binaryName string) (string, error) {
	path, err := exec.LookPath(binaryName)
	if err != nil {
		return "", fmt.Errorf("`%s` is not found in %%PATH%%", binaryName)
	}

	return path, nil
}

func BuildCommand(binaryPath string, args ...string) *exec.Cmd {
	cmd := exec.Command(binaryPath, args...)
	if IsDebugEnabled() {
		cmd.Stderr = os.Stderr
	}

	return cmd
}

func Sync() error {
	syncPath, err := GetBinary("sync")
	if err != nil {
		return err
	}

	syncCmd := BuildCommand(syncPath)
	if err := syncCmd.Run(); err != nil {
		return errors.New("cannot sync synchronize data on the disk with RAM using sync")
	}

	return nil
}

func IsDebugEnabled() bool {
	debugFlag, err := strconv.ParseBool(os.Getenv("YDB_BACKUP_TOOL_DEBUG"))
	if err != nil {
		debugFlag = false
	}
	return debugFlag
}
