# ydb-backup-tool

A tool that simplifies the process of incremental backup and restore for Yandex Database (YDB).
This tool is based on the deduplication mechanism of Btrfs file system.

## Features

* Easily create and restore backups of all or a specific set of tables.
* Efficient storing of multiple backups on the file system.
* Compression of the backups.
* The tool supports UNIX-like operating systems.
* File-level incrementality.
* Fail-safe backup process: the unsuccessful and uncompleted backups will be deleted automatically.

## Limitations

* Deduplication may struggle with certain data modifications.
* The tool requires root privileges to operate.
* External dependencies: *btrfs-progs (v5.4.1 or higher)*, *duperemove (v0.11.1 or higher)*, and *YDB CLI (v2.4.0 or higher)* are utilized by the tool.

## Installation

To install the tool, it is required to build it from the sources using Golang compiler.

The required Go version is 1.19 or higher.

To build from the sources:
```shell
go build -o ./ydb-backup-tool ./cmd/backup
```

## CLI commands

The tool supports 4 commands: create, restore, list, and list-sizes.

#### Create backup

```
NAME:
   ydb-backup-tool create - Create an incremental backup.

USAGE:
   ydb-backup-tool [--dedup-b=<block_size>] [--compress=<algorithm>] [--compress-level=<algorithm_level>] [--ydb-dump-path=<path>] [--ydb-dump-consistency-level=<level>] [--ydb-dump-exclude=<pattern>] [--ydb-dump-scheme-only] [--ydb-dump-avoid-copy] create

OPTIONS:
   --ydb-endpoint=value                     YDB endpoint.
   --ydb-name=value                         YDB database name.
   --ydb-yc-token-file=value                YDB OAuth token file.
   --ydb-iam-token-file=value               YDB IAM token file.
   --ydb-sa-key-file=value                  YDB Service Account Key file.
   --ydb-p=value                            YDB profile name.
   --ydb-use-metadata-credentials           YDB use the metadata service.
   --dedup-b=value                          Block size for reading file extents. Default is 4096 bytes.
   --compress=value                         Compression algorithm. Available: ZSTD, ZLIB, and LZO. Default is ZSTD.
   --compress-level=value                   Compression level. Default is 3.
   --ydb-dump-path=value                    Path to the database directory with objects or a path to the table to be dumped.The root database directory is used by default.
   --ydb-dump-consistency-level=value       The consistency level. Possible options: database and table. Default is database.
   --ydb-dump-exclude=value                 Template (PCRE) to exclude paths from export.
   --ydb-dump-scheme-only                   Dump only the details about the database schema objects, without dumping their data.
   --ydb-dump-avoid-copy                    Do not create a snapshot before dumping.
```

#### Restore from backup
```
NAME:
   ydb-backup-tool restore - Restore from an incremental backup.

USAGE:
   ydb-backup-tool [--ydb-restore-path=value] [--ydb-restore-data=value] [--ydb-restore-indexes=value] [--ydb-restore-dry-run] restore <backup_name>

OPTIONS:
   --ydb-endpoint=value                     YDB endpoint.
   --ydb-name=value                         YDB database name.
   --ydb-yc-token-file=value                YDB OAuth token file.
   --ydb-iam-token-file=value               YDB IAM token file.
   --ydb-sa-key-file=value                  YDB Service Account Key file.
   --ydb-p=value                            YDB profile name.
   --ydb-use-metadata-credentials           YDB use the metadata service.
   --ydb-restore-path=value                 Path to the database directory the data will be imported to. Default is the root directory. 
   --ydb-restore-data=value                 Enables/disables data import, 1 (yes) or 0 (no), defaults to 1.
   --ydb-restore-indexes=value              Enables/disables import of indexes, 1 (yes) or 0 (no), defaults to 1.
   --ydb-restore-dry-run                    Matching the data schemas in the database and file system without updating the database, 1 (yes) or 0 (no), defaults to 0.
```

#### List backups
```
NAME:
   ydb-backup-tool list - List of completed backups.

USAGE:
   ydb-backup-tool list

OPTIONS:
   --ydb-endpoint=value                     YDB endpoint.
   --ydb-name=value                         YDB database name.
   --ydb-yc-token-file=value                YDB OAuth token file.
   --ydb-iam-token-file=value               YDB IAM token file.
   --ydb-sa-key-file=value                  YDB Service Account Key file.
   --ydb-p=value                            YDB profile name.
   --ydb-use-metadata-credentials           YDB use the metadata service.
```

#### List backups information
```
NAME:
   ydb-backup-tool list-sizes - List of the meta information about backups (name and size).

USAGE:
   ydb-backup-tool list-sizes

OPTIONS:
   --ydb-endpoint=value                     YDB endpoint.
   --ydb-name=value                         YDB database name.
   --ydb-yc-token-file=value                YDB OAuth token file.
   --ydb-iam-token-file=value               YDB IAM token file.
   --ydb-sa-key-file=value                  YDB Service Account Key file.
   --ydb-p=value                            YDB profile name.
   --ydb-use-metadata-credentials           YDB use the metadata service.
```


## Contribution 
You can contribute to our project through pull requests - we are glad to new ideas and fixes.

## Credits

This project is developed by:
* [@mcflydesigner](https://github.com/mcflydesigner)
* [@e2xen](https://github.com/e2xen)

## License
The project is released and distributed under [MIT License](https://en.wikipedia.org/wiki/MIT_License).
