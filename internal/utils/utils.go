package utils

import (
	"fmt"
	"log"
	"os"
	"os/exec"
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

func CreateFile(name string) error {
	f, err := os.Create(name)
	if err != nil {
		return fmt.Errorf("failed to create file %s", err)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Printf("WARN: cannot close file descriptor of the file %s", name)
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

func IsDebugEnabled() bool {
	debugFlag, err := strconv.ParseBool(os.Getenv("YDB_BACKUP_TOOL_DEBUG"))
	if err != nil {
		debugFlag = false
	}
	return debugFlag
}
