package duperemove

import (
	"bytes"
	"errors"
	"strings"
	"ydb-backup-tool/internal/utils"
)

func DeduplicateDirectory(path string) error {
	duperemovePath, err := utils.GetBinary("duperemove")
	if err != nil {
		return err
	}

	duperemoveCmd := utils.BuildCommand(duperemovePath, "-dr", path)
	// TODO: refactoring
	var errBuffer bytes.Buffer
	duperemoveCmd.Stderr = &errBuffer

	if err := duperemoveCmd.Run(); err != nil {
		if !strings.Contains(errBuffer.String(), "No dedupe candidates found") {
			return errors.New("failed to perform data deduplication using `duperemove`")
		}
	}

	return nil
}
