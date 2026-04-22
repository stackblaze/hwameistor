package backup

// Backup / restore (restic) constants. These mirror the hwameistor.io
// label and annotation scheme used by the rest of the project.
const (
	// pluginName is the label/annotation prefix. Kept in sync with the
	// hwameistor CSI driver name root.
	pluginName = "hwameistor.io"

	// BackupConfigSingletonName is the only BackupConfig name the controller honors.
	BackupConfigSingletonName = "default"

	// CreatedbyLabelKey / CreatedbyLabelValue identify resources authored
	// by the backup controller.
	CreatedbyLabelKey   = "app.kubernetes.io/created-by"
	CreatedbyLabelValue = "hwameistor-backup-controller"

	// Annotation keys (on the source PVC) overriding BackupConfig defaults.
	BackupEnabledAnnotation          = pluginName + "/backup"
	BackupScheduleAnnotation         = pluginName + "/backup-schedule"
	BackupKeepSnapshotAnnotation     = pluginName + "/backup-keep-snapshot"
	BackupRetentionLastAnnotation    = pluginName + "/backup-retention-last"
	BackupRetentionHourlyAnnotation  = pluginName + "/backup-retention-hourly"
	BackupRetentionDailyAnnotation   = pluginName + "/backup-retention-daily"
	BackupRetentionWeeklyAnnotation  = pluginName + "/backup-retention-weekly"
	BackupRetentionMonthlyAnnotation = pluginName + "/backup-retention-monthly"
	BackupRetentionYearlyAnnotation  = pluginName + "/backup-retention-yearly"

	// BackupManagedByLabel marks CronJobs/Jobs/Snapshots created by the backup controller.
	BackupManagedByLabel         = pluginName + "/backup-managed-by"
	BackupManagedByValue         = "hwameistor-backup"
	BackupOwnerPVCNamespaceLabel = pluginName + "/backup-pvc-namespace"
	BackupOwnerPVCNameLabel      = pluginName + "/backup-pvc-name"

	// BackupPVCFinalizer blocks deletion of a backed-up PVC until its CronJob is removed.
	BackupPVCFinalizer = pluginName + "/backup"

	// Resource name suffixes.
	BackupCronJobSuffix    = "-backup"
	BackupJobSnapshotInfix = "-backup-snap-"
	BackupJobTempPVCInfix  = "-backup-src-"
	RestoreJobSuffix       = "-restore"

	// Secret key names.
	ResticPasswordSecretKey = "password"
	S3AccessKeyIDSecretKey  = "AWS_ACCESS_KEY_ID"
	S3SecretKeySecretKey    = "AWS_SECRET_ACCESS_KEY"

	// DefaultResticImage is the default image used for restic backup/restore Jobs.
	DefaultResticImage = "restic/restic:0.17.3"

	// HwameistorLVMCSIProvisioner is the CSI provisioner name used by the
	// lvm.hwameistor.io driver. Only PVCs backed by StorageClasses with this
	// provisioner are considered for backup.
	HwameistorLVMCSIProvisioner = "lvm.hwameistor.io"
)
