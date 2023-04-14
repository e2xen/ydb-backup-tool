package utils

import (
	"fmt"
	"os"
)

func CreateDirectory(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return fmt.Errorf("cannot create the following directory: %s. Error: %w", dir, err)
		}
	}
	return nil
}

func CreateFile(name *string) error {
	f, err := os.Create(*name)
	if err != nil {
		return fmt.Errorf("failed to create file %s", err)
	}
	// TODO: Error handler in defer?
	defer f.Close()
	return nil
}

func DeleteFile(path *string) error {
	if _, err := os.Stat(*path); !os.IsNotExist(err) {
		if err := os.Remove(*path); err != nil {
			return fmt.Errorf("failed to delete file %s", *path)
		}
	}
	return nil
}
