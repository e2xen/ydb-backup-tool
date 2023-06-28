package btrfs

import (
	"errors"
	"fmt"
	"golang.org/x/exp/slices"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"ydb-backup-tool/internal/utils"
)

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

type FsUsage struct {
	DeviceSize        int64
	DeviceAllocated   int64
	DeviceUnallocated int64
	Used              int64
	Free              int64
}

func NewSubvolume(path string, isSnapshot bool) *Subvolume {
	pathSplit := strings.Split(path, "/")
	return &Subvolume{Path: path, Name: pathSplit[len(pathSplit)-1], IsSnapshot: isSnapshot}
}

func NewSnapshot(path string) *Subvolume {
	return NewSubvolume(path, true)
}

func GetFileSystemUsage(path string) (*FsUsage, error) {
	// sudo btrfs filesystem usage -b -T /var/lib/ydb-backup-tool/mnt
	btrfsPath, err := utils.GetBinary("btrfs")
	if err != nil {
		return nil, err
	}

	cmd := utils.BuildCommand(btrfsPath, "filesystem", "usage", "-b", "-T", path)
	out, err := cmd.Output()
	if err != nil {
		return nil, errors.New("cannot obtain btrfs usage statistics")
	}

	metaMap := make(map[string]string)
	for _, line := range strings.Split(string(out), "\n") {
		cuttingByDelimiter := strings.SplitN(line, ":", 2)

		if len(cuttingByDelimiter) == 2 {
			key := strings.ToLower(strings.TrimSpace(cuttingByDelimiter[0]))
			value := strings.TrimSpace(cuttingByDelimiter[1])
			metaMap[key] = value
		}
	}

	devSize, err := strconv.ParseInt(metaMap["device size"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse device size from btrfs usage of `%s`", path)
	}
	devAllocated, err := strconv.ParseInt(metaMap["device allocated"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse device allocated from btrfs usage of `%s`", path)
	}
	devUnallocated, err := strconv.ParseInt(metaMap["device unallocated"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse device unallocated from btrfs usage of `%s`", path)
	}
	used, err := strconv.ParseInt(metaMap["used"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse used space from btrfs usage of `%s`", path)
	}
	var free int64
	n, err := fmt.Sscanf(metaMap["free (estimated)"], "%d", &free)
	if err != nil || n != 1 {
		return nil, fmt.Errorf("failed to parse free space from btrfs usage of `%s`", path)
	}

	return &FsUsage{DeviceSize: devSize, DeviceAllocated: devAllocated, DeviceUnallocated: devUnallocated, Used: used, Free: free}, nil
}

func MakeBtrfsFileSystem(filePath string) error {
	mkfsPath, err := utils.GetBinary("mkfs.btrfs")
	if err != nil {
		return err
	}
	mkfsCmd := utils.BuildCommand(mkfsPath, filePath)
	if err := mkfsCmd.Run(); err != nil {
		return fmt.Errorf("failed to initialize btrfs in the file `%s`", filePath)
	}

	return nil
}

// CreateSubvolume /* It will not work with recursive subvolumes */
func CreateSubvolume(path string) (*Subvolume, error) {
	btrfsPath, err := utils.GetBinary("btrfs")
	if err != nil {
		return nil, err
	}

	btrfsCmd := utils.BuildCommand(btrfsPath, "subvolume", "create", path)
	if err := btrfsCmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to create subvolume `%s`", path)
	}

	return NewSubvolume(path, false), nil
}

func CreateSnapshot(subvolume *Subvolume, snapshotTargetPath string) (*Subvolume, error) {
	subvolumeExists, err := verifySubvolumeExists(subvolume)

	if err != nil {
		return nil, errors.New("cannot verify that subvolume exists")
	}
	if !subvolumeExists {
		return nil, fmt.Errorf("cannot find subvolume `%s`: %w", subvolume.Path, err)
	}

	btrfsPath, err := utils.GetBinary("btrfs")
	if err != nil {
		return nil, err
	}

	btrfsCmd := utils.BuildCommand(btrfsPath, "subvolume", "snapshot", "-r", subvolume.Path, snapshotTargetPath)
	if err := btrfsCmd.Run(); err != nil {
		return nil, fmt.Errorf("cannot create snapshot %s", snapshotTargetPath)
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
	btrfsCmd := utils.BuildCommand(btrfsPath, "subvolume", "list", "-o", path)
	out, err := btrfsCmd.Output()
	if err != nil {
		return nil, errors.New("cannot get list of subvolumes")
	}

	for _, subvolume := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(subvolume) != "" {
			words := strings.Split(subvolume, " ")
			name := filepath.Base(words[len(words)-1])
			subvolumePath := path + "/" + name

			// If the subvolume is already in the list, then it is a snapshot - ignore it in the loop
			if !slices.Contains(result, NewSnapshot(subvolumePath)) {
				result = append(result, NewSubvolume(subvolumePath, false))
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

	btrfsCmd := utils.BuildCommand(btrfsPath, "subvolume", "list", "-r", path)
	out, err := btrfsCmd.Output()
	if err != nil {
		return nil, fmt.Errorf("cannot get list of snapshots")
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
		return nil, errors.New("cannot get list of snapshots")
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
		return nil, errors.New("cannot get list of subvolumes")
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
		return fmt.Errorf("failed to verify the existence of the following subvolume `%s`", subvolume.Path)
	}
	if !subvolumeExists {
		return fmt.Errorf("subvolume %s does not exist", subvolume.Path)
	}

	btrfsCmd := utils.BuildCommand(btrfsPath, "subvolume", "delete", subvolume.Path)
	if err := btrfsCmd.Run(); err != nil {
		return fmt.Errorf("failed to delete the following subvolume `%s`", subvolume.Path)
	}

	return nil
}

func GetSubvolumesMeta(path string) (*[]SubvolumeMeta, error) {
	subvolumes, err := GetSubvolumes(path)
	if err != nil {
		return nil, err
	}

	if err := quotaGroupEnable(path); err != nil {
		return nil, fmt.Errorf("failed to enable quota group for the given path `%s`", path)
	}

	btrfsPath, err := utils.GetBinary("btrfs")
	if err != nil {
		return nil, err
	}

	var result []SubvolumeMeta

	for _, subvolume := range subvolumes {
		cmd := utils.BuildCommand(btrfsPath, "subvolume", "show", "-b", subvolume.Path)
		out, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("failed to get meta information about the following subvolume `%s`", subvolume.Path)
		}

		subvolumeMeta, err := extractSubvolumeMetaInfo(string(out), NewSubvolume(subvolume.Path, false))
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

func VerifySubvolumeExists(path string) (bool, error) {
	subvolume, err := GetSubvolume(path)
	if err != nil {
		return false, fmt.Errorf("cannot get list of subvolumes: %w", err)
	}

	if subvolume != nil {
		return true, nil
	}
	return false, nil
}

func ResizeFileSystem(path string, newSize string) error {
	btrfsPath, err := utils.GetBinary("btrfs")
	if err != nil {
		return err
	}

	cmd := utils.BuildCommand(btrfsPath, "filesystem", "resize", newSize, path)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to resize btrfs %s", path)
	}

	return nil
}

func SetProperty(path string, key string, value string) error {
	btrfsPath, err := utils.GetBinary("btrfs")
	if err != nil {
		return err
	}

	cmd := utils.BuildCommand(btrfsPath, "property", "set", path, key, value)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to set property %s = %s for the given path %s", key, value, path)
	}
	return nil
}

func verifySubvolumeExists(subvolume *Subvolume) (bool, error) {
	dir := filepath.Dir(subvolume.Path)

	subvolumes, err := GetSubvolumes(dir)
	if err != nil {
		return false, errors.New("cannot get list of subvolumes")
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

	btrfsCmd := utils.BuildCommand(btrfsPath, "quota", "enable", path)
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
