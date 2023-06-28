package compression

import (
	"fmt"
	"ydb-backup-tool/internal/btrfs"
)

type Algorithm string

const (
	Zlib Algorithm = "zlib"
	Lzo  Algorithm = "lzo"
	Zstd Algorithm = "zstd"
)

var mapMaxCompressionLevel = map[Algorithm]uint64{
	Zlib: 9,
	Lzo:  1,
	Zstd: 15,
}

type compression struct {
	algorithm           Algorithm
	compressionLevel    uint64
	maxCompressionLevel uint64
}

func (c compression) Algorithm() Algorithm {
	return c.algorithm
}

func (c compression) CompressionLevel() uint64 {
	return c.compressionLevel
}

func (c compression) MaxCompressionLevel() uint64 {
	return c.maxCompressionLevel
}

type Compression interface {
	Algorithm() Algorithm
	CompressionLevel() uint64
	MaxCompressionLevel() uint64
}

func CreateCompression(algorithm Algorithm, compressionLevel uint64) (Compression, error) {
	switch algorithm {
	case Zlib, Lzo, Zstd:
		if compressionLevel > 0 && compressionLevel <= mapMaxCompressionLevel[algorithm] {
			return compression{algorithm, compressionLevel, mapMaxCompressionLevel[algorithm]}, nil
		}
		return nil, fmt.Errorf("wrong compression level passed for the compression algorithm. Supports value from 1 to %d",
			mapMaxCompressionLevel[algorithm])

	default:
		return nil, fmt.Errorf("wrong compression algorithm is passed. Expected: %s, %s, %s. Got: %s",
			Zlib, Lzo, Zstd, algorithm)
	}
}

func EnableCompression(path string, compression Compression) error {
	if err := btrfs.SetProperty(path, "compression", string(compression.Algorithm())); err != nil {
		return fmt.Errorf("failed to enable compression for the given path `%s`: %s", path, err)
	}

	return nil
}
