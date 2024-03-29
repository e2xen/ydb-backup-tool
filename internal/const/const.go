package _const

const CompressionAlgorithmArg = "compress"
const CompressionLevelArg = "compress-level"
const DedupBlockSize = "dedup-b"
const YdbEndpointArg = "ydb-endpoint"
const YdbNameArg = "ydb-name"
const YdbYcTokenFileArg = "ydb-yc-token-file"
const YdbIamTokenFileArg = "ydb-iam-token-file"
const YdbSaKeyFileArg = "ydb-sa-key-file"
const YdbProfileArg = "ydb-p"
const YdbUseMetadataCredsArg = "ydb-use-metadata-credentials"
const YdbDumpPath = "ydb-dump-path"
const YdbDumpExclude = "ydb-dump-exclude"
const YdbDumpSchemeOnly = "ydb-dump-scheme-only"
const YdbDumpConsistencyLevel = "ydb-dump-consistency-level"
const YdbDumpAvoidCopy = "ydb-dump-avoid-copy"
const YdbRestorePath = "ydb-restore-path"
const YdbRestoreData = "ydb-restore-data"
const YdbRestoreIndexes = "ydb-restore-indexes"
const YdbRestoreDryRun = "ydb-restore-dry-run"

const AppDataPath = "/var/lib/ydb-backup-tool"
const AppTmpPath = AppDataPath + "/tmp"
const AppMetaPath = AppDataPath + "/meta.json"
const AppHashfilePath = AppDataPath + "/hashfile"
const AppBaseDataBackingFilePath = AppDataPath + "/data.img"
const AppDataMountPath = AppDataPath + "/mnt"
const AppBackupsPath = AppDataMountPath + "/backups"
