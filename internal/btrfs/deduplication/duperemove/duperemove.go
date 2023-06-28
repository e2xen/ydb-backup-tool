package duperemove

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	_const "ydb-backup-tool/internal/const"
	"ydb-backup-tool/internal/utils"
)

type Params struct {
	BlockSize uint64
}

func DeduplicateDirectory(path string, params *Params) error {
	duperemovePath, err := utils.GetBinary("duperemove")
	if err != nil {
		return err
	}

	duperemoveCmd := utils.BuildCommand(duperemovePath, "-dr", "-b", strconv.FormatUint(params.BlockSize, 10),
		"--lookup-extents=yes", fmt.Sprintf("--hashfile=%s", _const.AppHashfilePath), path)

	var errBuffer bytes.Buffer
	duperemoveCmd.Stderr = &errBuffer

	if err := duperemoveCmd.Run(); err != nil {
		if !strings.Contains(errBuffer.String(), "No dedupe candidates found") {
			return errors.New("failed to perform data deduplication using `duperemove`")
		}
	}

	return nil
}
