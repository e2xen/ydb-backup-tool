package _const

const YdbEndpointArg = "ydb-endpoint"
const YdbNameArg = "ydb-name"
const YdbYcTokenFileArg = "ydb-yc-token-file"
const YdbIamTokenFileArg = "ydb-iam-token-file"
const YdbSaKeyFileArg = "ydb-sa-key-file"
const YdbProfileArg = "ydb-p"
const YdbUseMetadataCredsArg = "ydb-use-metadata-credentials"

const AppPath = "/var/lib/ydb-backup-tool"
const AppMountPath = AppPath + "/mnt"
const AppSnapshotsFolderName = "snapshots"
const AppSnapshotsFolderPath = AppMountPath + "/" + AppSnapshotsFolderName
