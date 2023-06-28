package command

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"ydb-backup-tool/internal/btrfs"
	comp "ydb-backup-tool/internal/btrfs/compression"
	"ydb-backup-tool/internal/btrfs/deduplication/duperemove"
	_const "ydb-backup-tool/internal/const"
	"ydb-backup-tool/internal/device"
	"ydb-backup-tool/internal/meta"
	"ydb-backup-tool/internal/utils"
	_math "ydb-backup-tool/internal/utils/math"
	"ydb-backup-tool/internal/ydb"
)

type Command int64

const (
	ListAllBackups Command = iota
	ListAllBackupsSizes
	CreateIncrementalBackup
	RestoreFromBackup
)

func (command *Command) ListBackups(mountPoint *device.MountPoint) error {
	if err := syncSubvolumesWithMeta(); err != nil {
		return err
	}

	backupsSubvolume, err := getOrCreateBackupsSubvolume()
	if err != nil {
		return fmt.Errorf("failed to get subvolume with backups: %w", err)
	}

	if err := utils.Sync(); err != nil {
		return err
	}

	metaBackups, err := meta.GetBackups()
	if err != nil {
		return fmt.Errorf("failed to get backups meta information: %w", err)
	}

	var atLeastOneBackupCompleted bool
	for _, metaBackup := range *metaBackups {
		if metaBackup.Completed == true {
			atLeastOneBackupCompleted = true
			break
		}
	}

	if !atLeastOneBackupCompleted {
		fmt.Printf("Currently, there is no backups")
	} else {
		subvolumes, err := btrfs.GetSubvolumes(backupsSubvolume.Path)
		if err != nil {
			return fmt.Errorf("cannot get list of subvolumes: %w", err)
		}

		var subvolumesMap = map[string]*btrfs.Subvolume{}
		for _, subvolume := range subvolumes {
			subvolumesMap[subvolume.Path] = subvolume
		}

		w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)
		fmt.Fprintln(w, "#\tName\t")
		for i, metaBackup := range *metaBackups {
			if val, ok := subvolumesMap[metaBackup.Path]; ok && metaBackup.Completed {
				fmt.Fprintln(w, fmt.Sprintf("%d\t%s\t", i, val.Name))
			}
		}

		if err := w.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (command *Command) ListBackupsSizes(mountPoint *device.MountPoint) error {
	if err := syncSubvolumesWithMeta(); err != nil {
		return err
	}

	backupsSubvolume, err := getOrCreateBackupsSubvolume()
	if err != nil {
		return fmt.Errorf("failed to get subvolume with backups: %w", err)
	}

	if err := utils.Sync(); err != nil {
		return err
	}

	metaBackups, err := meta.GetBackups()
	if err != nil {
		return fmt.Errorf("failed to get backups meta information: %w", err)
	}

	var atLeastOneBackupCompleted bool
	for _, metaBackup := range *metaBackups {
		if metaBackup.Completed == true {
			atLeastOneBackupCompleted = true
			break
		}
	}

	if !atLeastOneBackupCompleted {
		log.Printf("Currently, there is no backups\n")
	} else {
		metaSubvolumes, err := btrfs.GetSubvolumesMeta(backupsSubvolume.Path)
		if err != nil {
			return fmt.Errorf("failed to get meta information about subvolumes: %w", err)
		}

		sort.Slice(*metaSubvolumes, func(i, j int) bool {
			return (*metaSubvolumes)[i].Id < (*metaSubvolumes)[j].Id
		})
		metaSubvolumeMap := map[string]btrfs.SubvolumeMeta{}
		for _, metaSubvolume := range *metaSubvolumes {
			metaSubvolumeMap[metaSubvolume.Base.Path] = metaSubvolume
		}

		w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)
		fmt.Fprintln(w, "Id\tBackup Name\tUsage referenced\tUsage exclusive\t")
		for _, metaBackup := range *metaBackups {
			if val, ok := metaSubvolumeMap[metaBackup.Path]; ok && metaBackup.Completed {
				sizeReferencedKb := float64(val.SizeReferenced) / 1024
				sizeExclusiveKb := float64(val.SizeExclusive) / 1024
				fmt.Fprintln(w, fmt.Sprintf("%d\t%s\t%.2fKb\t%.2fKb\t",
					val.Id, val.Base.Name, sizeReferencedKb, sizeExclusiveKb))
			}
		}

		if err := w.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func (command *Command) CreateIncrementalBackup(
	mountPoint *device.MountPoint,
	ydbParams *ydb.YdbParams,
	dumpParams *ydb.DumpParams,
	compression *comp.Compression,
	dedupParams *duperemove.Params) error {
	if err := syncSubvolumesWithMeta(); err != nil {
		return err
	}

	backupsSubvolume, err := getOrCreateBackupsSubvolume()
	if err != nil {
		return fmt.Errorf("failed to get subvolume with backups: %w", err)
	}

	targetPath := backupsSubvolume.Path + "/ydb_backup_" + strconv.Itoa(int(time.Now().Unix()))
	subvolume, err := createFullBackupSubvolume(mountPoint, ydbParams, dumpParams, compression, targetPath)
	if err != nil {
		return fmt.Errorf("cannot perform full backup: %w", err)
	}

	if err := duperemove.DeduplicateDirectory(backupsSubvolume.Path, dedupParams); err != nil {
		return err
	}

	fmt.Printf("Successfully performed incremental backup!\nPath: %s\n", subvolume.Path)
	return nil
}

func (command *Command) RestoreFromBackup(mountPoint *device.MountPoint,
	ydbParams *ydb.YdbParams,
	restoreParams *ydb.RestoreParams,
	sourcePath string) error {
	if err := syncSubvolumesWithMeta(); err != nil {
		return err
	}

	finalSourcePath := strings.TrimSpace(sourcePath)
	if !strings.HasPrefix(finalSourcePath, "/") {
		finalSourcePath = "/" + finalSourcePath
	}
	if !strings.HasPrefix(finalSourcePath, _const.AppBackupsPath) {
		finalSourcePath = _const.AppBackupsPath + finalSourcePath
	}

	subvolumeExists, err := btrfs.VerifySubvolumeExists(finalSourcePath)
	if err != nil {
		return fmt.Errorf("cannot obtain info about backup from `%s`", sourcePath)
	}
	if !subvolumeExists {
		return fmt.Errorf("cannot find backup `%s`", sourcePath)
	}

	if err := ydb.Restore(ydbParams, restoreParams, finalSourcePath); err != nil {
		return fmt.Errorf("failed to restore from the backup `%s`: %w", sourcePath, err)
	}

	fmt.Printf("Successfully restored from the backup `%s`!\n", sourcePath)

	return nil
}

func createFullBackupSubvolume(
	mountPoint *device.MountPoint,
	ydbParams *ydb.YdbParams,
	dumpParams *ydb.DumpParams,
	compression *comp.Compression,
	targetPath string) (*btrfs.Subvolume, error) {
	if err := utils.CreateDirectory(_const.AppTmpPath); err != nil {
		return nil, fmt.Errorf("failed to create directory `%s`", _const.AppTmpPath)
	}

	tempBackupPath := _const.AppTmpPath + "/temp_backup_" + strconv.Itoa(int(time.Now().Unix()))
	if err := utils.CreateDirectory(tempBackupPath); err != nil {
		return nil, fmt.Errorf("failed to create a temporary directory for backup `%s`", tempBackupPath)
	}
	defer func() {
		if err := utils.DeleteDirectory(tempBackupPath); err != nil {
			log.Warnf("failed to delete temporary backup directory `%s`", tempBackupPath)
		}
	}()

	if err := meta.StartBackup(targetPath); err != nil {
		return nil, err
	}

	backup, err := ydb.Dump(ydbParams, dumpParams, tempBackupPath)
	if err != nil {
		return nil, fmt.Errorf("error occurred during YDB backup process: %w", err)
	}

	backupSize, err := utils.GetDirectorySize(backup.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to get size of `%s`: %w", backup.Path, err)
	}

	metaSize, err := btrfs.GetFileSystemUsage(mountPoint.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to get btrfs usage info: %w", err)
	}

	// Also, we should have 16Kib of free space to store subvolume metadata
	btrfsFreeSpace := metaSize.Free - 16*1024
	sizeDiff := btrfsFreeSpace - backupSize
	if sizeDiff < 0 {
		// Extend backing file size
		extendBy := 2 * _math.Abs(sizeDiff)

		if err := device.DetachLoopDevice(&mountPoint.LoopDev); err != nil {
			return nil, fmt.Errorf("failed to detach loop device %s", mountPoint.LoopDev.Name)
		}
		if err := device.Unmount(mountPoint); err != nil {
			return nil, fmt.Errorf("failed to unmount %s", mountPoint.Path)
		}
		if err := device.ExtendBackingStoreFileBy(&mountPoint.LoopDev.BackFile, _math.Abs(extendBy)); err != nil {
			return nil, fmt.Errorf("failed to extend backing store file: %w", err)
		}

		newLoopDev, err := device.SetupLoopDevice(&mountPoint.LoopDev.BackFile)
		if err != nil {
			log.Panicf("Cannot create a new loop device. %v", err)
		}
		newMountPoint, err := device.MountLoopDevice(newLoopDev, mountPoint.Path, compression)
		if err != nil {
			return nil, fmt.Errorf("failed to mount %s", mountPoint.Path)
		}
		mountPoint = newMountPoint
		if err := btrfs.ResizeFileSystem(mountPoint.Path, "max"); err != nil {
			return nil, err
		}
	}

	subvolume, err := btrfs.CreateSubvolume(targetPath)
	if err != nil {
		return nil, err
	}
	if compression != nil {
		if err := comp.EnableCompression(subvolume.Path, *compression); err != nil {
			return nil, err
		}
	}

	if err := utils.MoveFilesFromDirToDir(backup.Path, subvolume.Path); err != nil {
		return nil, err
	}

	if err := meta.FinishBackup(targetPath); err != nil {
		return nil, err
	}

	return subvolume, nil
}

func getOrCreateBackupsSubvolume() (*btrfs.Subvolume, error) {
	subvolume, err := btrfs.GetSubvolume(_const.AppBackupsPath)
	if err != nil {
		return nil, fmt.Errorf("cannot obtain info to verify that subvolume with backups exists: %w", err)
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

func syncSubvolumesWithMeta() error {
	backupsSubvolume, err := getOrCreateBackupsSubvolume()
	if err != nil {
		return fmt.Errorf("failed to get subvolume with backups: %w", err)
	}

	subvolumes, err := btrfs.GetSubvolumes(backupsSubvolume.Path)
	if err != nil {
		return err
	}

	metaBackups, err := meta.GetCompletedBackups()
	if err != nil {
		return err
	}
	metaBackupsPaths := utils.Map(*metaBackups, func(b meta.Backup) string { return b.Path })
	metaBackupsSet := make(map[string]bool)
	for _, backupPath := range metaBackupsPaths {
		metaBackupsSet[backupPath] = true
	}

	for _, subvolume := range subvolumes {
		if exists := metaBackupsSet[subvolume.Path]; !exists {
			log.Warnf("Deleting non-completed backup or an unknown subvolume `%s`", subvolume.Name)

			if err := btrfs.DeleteSubvolume(subvolume); err != nil {
				return err
			}
		}
	}

	return nil
}
