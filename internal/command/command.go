package command

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
	"ydb-backup-tool/internal/btrfs"
	_const "ydb-backup-tool/internal/const"
	"ydb-backup-tool/internal/ydb"
)

type Command int64

const (
	Undefined Command = iota
	ListAllBackups
	CreateFullBackup
	CreateIncrementalBackup
)

func (command *Command) ListBackups(mountPoint *btrfs.MountPoint) error {
	snapshotsSubvolume, err := getOrCreateSnapshotsSubvolume()
	if err != nil {
		return fmt.Errorf("failed to get subvolume with snapshots. Error: %w", err)
	}

	subvolumes, err := btrfs.GetSnapshots(snapshotsSubvolume.Path)
	if err != nil {
		return fmt.Errorf("cannot get list of subvolumes. Error: %w", err)
	}

	for i, subvolume := range subvolumes {
		log.Printf("%d %s\n", i, subvolume.Path)
	}
	return nil
}

func (command *Command) CreateFullBackup(mountPoint *btrfs.MountPoint, ydbParams *ydb.Params) error {
	snapshotsSubvolume, err := getOrCreateSnapshotsSubvolume()
	if err != nil {
		return fmt.Errorf("failed to get subvolume with snapshots. Error: %w", err)
	}

	targetPath := snapshotsSubvolume.Path + "/ydb_backup_" + strconv.Itoa(int(time.Now().Unix()))
	snapshot, err := createFullBackupSnapshot(targetPath, ydbParams)
	if err != nil {
		return fmt.Errorf("cannot perform full backup. Error: %w", err)
	}

	log.Print("Successfully performed full backup!")
	log.Print("path: " + snapshot.Path)

	return nil
}

func (command *Command) CreateIncrementalBackup(mountPoint *btrfs.MountPoint, ydbParams *ydb.Params, basePath string) error {
	finalBasePath := strings.TrimSpace(basePath)
	if !strings.HasPrefix(finalBasePath, "/") {
		finalBasePath = "/" + finalBasePath
	}
	if !strings.HasPrefix(finalBasePath, _const.AppSnapshotsFolderPath) {
		finalBasePath = _const.AppSnapshotsFolderPath + finalBasePath
	}

	baseExists, err := verifySnapshotExists(finalBasePath)
	if err != nil {
		return fmt.Errorf("cannot obtain info about backup `%s`", basePath)
	}
	if !baseExists {
		return fmt.Errorf("cannot find base backup: %s", finalBasePath)
	}

	snapshotsSubvolume, err := getOrCreateSnapshotsSubvolume()
	if err != nil {
		return fmt.Errorf("failed to get subvolume with snapshots. Error: %w", err)
	}

	targetPath := snapshotsSubvolume.Path + "/ydb_backup_" + strconv.Itoa(int(time.Now().Unix()))
	snapshotNew, err := createFullBackupSnapshot(targetPath, ydbParams)
	if err != nil {
		return fmt.Errorf("cannot perform full backup. Error: %w", err)
	}

	snapshotFinal, err := btrfs.CreateIncrementalSnapshot(
		&btrfs.Snapshot{Path: finalBasePath},
		&btrfs.Snapshot{Path: snapshotNew.Path},
		snapshotsSubvolume)
	if err != nil {
		return err
	}

	log.Print("Successfully performed incremental backup!")
	log.Print("path: " + snapshotFinal.Path)

	return nil
}

func createFullBackupSnapshot(targetPath string, ydbParams *ydb.Params) (*btrfs.Snapshot, error) {
	// Create temp subvolume
	subvolumeName := targetPath + "_temp_subvol"
	subvolume, err := btrfs.CreateSubvolume(subvolumeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create a temp subvolume %s", subvolumeName)
	}
	defer func() {
		// Delete temp subvolume
		err = btrfs.DeleteSubvolume(subvolume)
		if err != nil {
			log.Printf("failed to delete temp subvolume `%s`. Error: %w", subvolumeName, err)
		}
	}()

	_, err = ydb.Dump(ydbParams, subvolume.Path)
	if err != nil {
		return nil, fmt.Errorf("error occurred during YDB backup process: %w", err)
	}

	// Create snapshot from the temp subvolume
	_, err = btrfs.CreateSnapshot(subvolume, targetPath)
	if err != nil {
		return nil, fmt.Errorf("cannot create snapshot %s for the subvolume %s. Error: %w", targetPath, subvolume.Path, err)
	}

	return &btrfs.Snapshot{Path: targetPath}, nil
}

/* Utils */
func verifySnapshotExists(path string) (bool, error) {
	snapshot, err := btrfs.GetSnapshot(path)
	if err != nil {
		return false, fmt.Errorf("cannot get list of subvolumes. Error: %w", err)
	}

	if snapshot != nil {
		return true, nil
	}
	return false, nil
}

func getOrCreateSnapshotsSubvolume() (*btrfs.Subvolume, error) {
	appSnapshotsPath := _const.AppSnapshotsFolderPath
	subvolume, err := btrfs.GetSubvolume(appSnapshotsPath)
	if err != nil {
		return nil, fmt.Errorf("cannot obtain info to verify that subvolume with snapshots exists. Error: %w", err)
	}

	if subvolume == nil {
		subvolume, err := btrfs.CreateSubvolume(appSnapshotsPath)
		if err != nil {
			return nil, err
		}
		return subvolume, nil
	}

	return subvolume, nil
}
