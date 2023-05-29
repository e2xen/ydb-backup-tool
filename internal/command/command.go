package command

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"ydb-backup-tool/internal/btrfs"
	"ydb-backup-tool/internal/btrfs/deduplication/duperemove"
	_const "ydb-backup-tool/internal/const"
	"ydb-backup-tool/internal/device"
	"ydb-backup-tool/internal/utils"
	_math "ydb-backup-tool/internal/utils/math"
	"ydb-backup-tool/internal/ydb"
)

type Command int64

const (
	Undefined Command = iota
	ListAllBackups
	ListAllBackupsSizes
	CreateFullBackup
	CreateIncrementalBackup
	RestoreFromBackup
)

func (command *Command) ListBackups(mountPoint *device.MountPoint) error {
	backupsSubvolume, err := getOrCreateBackupsSubvolume()
	if err != nil {
		return fmt.Errorf("failed to get subvolume with backups. Error: %w", err)
	}

	if err := utils.Sync(); err != nil {
		return err
	}

	subvolumes, err := btrfs.GetSubvolumes(backupsSubvolume.Path)
	if err != nil {
		return fmt.Errorf("cannot get list of subvolumes. Error: %w", err)
	}

	if len(subvolumes) == 0 {
		fmt.Printf("Currently, there is no backups")
	}

	for i, subvolume := range subvolumes {
		fmt.Printf("%d %s\n", i, subvolume.Path)
	}
	return nil
}

func (command *Command) ListBackupsSizes(mountPoint *device.MountPoint) error {
	backupsSubvolume, err := getOrCreateBackupsSubvolume()
	if err != nil {
		return fmt.Errorf("failed to get subvolume with backups. Error: %w", err)
	}

	if err := utils.Sync(); err != nil {
		return err
	}

	metaSubvolumes, err := btrfs.GetSubvolumesMeta(backupsSubvolume.Path)
	if err != nil {
		return fmt.Errorf("failed to get meta information about subvolumes. Error: %w", err)
	}

	if len(*metaSubvolumes) == 0 {
		log.Printf("Currently, there is no backups")
	} else {
		sort.Slice(*metaSubvolumes, func(i, j int) bool {
			return (*metaSubvolumes)[i].Id < (*metaSubvolumes)[j].Id
		})
		w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)
		fmt.Fprintln(w, "Id\tBackup Name\tUsage referenced\tUsage exclusive\t")
		for _, metaSubvolume := range *metaSubvolumes {
			sizeReferencedKb := float64(metaSubvolume.SizeReferenced) / 1024
			sizeExclusiveKb := float64(metaSubvolume.SizeExclusive) / 1024
			fmt.Fprintln(w, fmt.Sprintf("%d\t%s\t%.2fKb\t%.2fKb\t", metaSubvolume.Id, metaSubvolume.Base.Name, sizeReferencedKb, sizeExclusiveKb))
		}

		if err := w.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func (command *Command) CreateFullBackup(mountPoint *device.MountPoint, ydbParams *ydb.Params) error {
	backupsSubvolume, err := getOrCreateBackupsSubvolume()
	if err != nil {
		return fmt.Errorf("failed to get subvolume with backups. Error: %w", err)
	}

	targetPath := backupsSubvolume.Path + "/ydb_backup_" + strconv.Itoa(int(time.Now().Unix()))
	snapshot, err := createFullBackupSnapshot(mountPoint, ydbParams, targetPath)
	if err != nil {
		return fmt.Errorf("cannot perform full backup. Error: %w", err)
	}

	log.Print("Successfully performed full backup!")
	log.Print("path: " + snapshot.Path)

	return nil
}

func (command *Command) CreateIncrementalBackup(mountPoint *device.MountPoint, ydbParams *ydb.Params) error {
	backupsSubvolume, err := getOrCreateBackupsSubvolume()
	if err != nil {
		return fmt.Errorf("failed to get subvolume with backups. Error: %w", err)
	}

	targetPath := backupsSubvolume.Path + "/ydb_backup_" + strconv.Itoa(int(time.Now().Unix()))
	snapshot, err := createFullBackupSnapshot(mountPoint, ydbParams, targetPath)
	if err != nil {
		return fmt.Errorf("cannot perform full backup. Error: %w", err)
	}

	if err := duperemove.DeduplicateDirectory(backupsSubvolume.Path); err != nil {
		return err
	}

	log.Print("Successfully performed incremental backup using deduplication!")
	log.Printf("Path: %s", snapshot.Path)

	return nil
}

func (command *Command) RestoreFromBackup(mountPoint *device.MountPoint, ydbParams *ydb.Params, sourcePath string) error {
	finalSourcePath := strings.TrimSpace(sourcePath)
	if !strings.HasPrefix(finalSourcePath, "/") {
		finalSourcePath = "/" + finalSourcePath
	}
	if !strings.HasPrefix(finalSourcePath, _const.AppBackupsPath) {
		finalSourcePath = _const.AppBackupsPath + finalSourcePath
	}

	subvolumeExists, err := verifySubvolumeExists(finalSourcePath)
	if err != nil {
		return fmt.Errorf("cannot obtain info about backup: %s", sourcePath)
	}
	if !subvolumeExists {
		return fmt.Errorf("cannot find backup: %s", sourcePath)
	}

	if err := ydb.Restore(ydbParams, finalSourcePath); err != nil {
		return fmt.Errorf("failed to restore from the backup `%s`: %w", sourcePath, err)
	}

	log.Printf("Successfully restored from the backup `%s`!", sourcePath)

	return nil
}

func createFullBackupSnapshot(mountPoint *device.MountPoint, ydbParams *ydb.Params, targetPath string) (*btrfs.Subvolume, error) {
	err := utils.CreateDirectory(_const.AppTmpPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory `%s`", _const.AppTmpPath)
	}

	tempBackupPath := _const.AppTmpPath + "/temp_backup_" + strconv.Itoa(int(time.Now().Unix()))
	if err := utils.CreateDirectory(tempBackupPath); err != nil {
		return nil, fmt.Errorf("failed to create a temporary directory for backup `%s`", tempBackupPath)
	}
	defer func() {
		if err := utils.DeleteFile(tempBackupPath); err != nil {
			log.Printf("WARN: failed to delete temporary backup directory: %s", tempBackupPath)
		}
	}()
	_, err = ydb.Dump(ydbParams, tempBackupPath)
	if err != nil {
		return nil, fmt.Errorf("error occurred during YDB backup process: %s", err)
	}

	backupSize, err := utils.GetDirectorySize(tempBackupPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get size of `%s`: %s", tempBackupPath, err)
	}

	meta, err := btrfs.GetFileSystemUsage(mountPoint.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to get btrfs usage info: %s", err)
	}

	// Also, we should have 16Kib of free space to store subvolume metadata
	btrfsFreeSpace := meta.Free - 16*1024
	sizeDiff := btrfsFreeSpace - backupSize
	if sizeDiff < 0 {
		// Extend backing file size
		extendBy := _math.Abs(sizeDiff)
		if err := device.ExtendBackingStoreFileBy(&mountPoint.LoopDev.BackFile, _math.Abs(extendBy)); err != nil {

			fmt.Printf("Extending by %d", extendBy)
			return nil, fmt.Errorf("failed to extend backing store file: %s", err)
		}

		if err := btrfs.ResizeFileSystem(mountPoint.Path, "max"); err != nil {
			return nil, err
		}
	}

	subvolume, err := btrfs.CreateSubvolume(targetPath)
	if err != nil {
		return nil, err
	}

	if err := utils.MoveFilesFromDirToDir(tempBackupPath, subvolume.Path); err != nil {
		return nil, err
	}

	return subvolume, nil
}

func verifySubvolumeExists(path string) (bool, error) {
	subvolume, err := btrfs.GetSubvolume(path)
	if err != nil {
		return false, fmt.Errorf("cannot get list of subvolumes. Error: %w", err)
	}

	if subvolume != nil {
		return true, nil
	}
	return false, nil
}

func getOrCreateBackupsSubvolume() (*btrfs.Subvolume, error) {
	subvolume, err := btrfs.GetSubvolume(_const.AppBackupsPath)
	if err != nil {
		return nil, fmt.Errorf("cannot obtain info to verify that subvolume with backup exists. Error: %w", err)
	}

	if subvolume == nil {
		subvolume, err := btrfs.CreateSubvolume(_const.AppBackupsPath)
		if err != nil {
			return nil, err
		}
		return subvolume, nil
	}

	return subvolume, nil
}
