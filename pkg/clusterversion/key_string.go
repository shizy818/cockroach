// Code generated by "stringer"; DO NOT EDIT.

package clusterversion

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[V21_2-0]
	_ = x[Start22_1-1]
	_ = x[TargetBytesAvoidExcess-2]
	_ = x[AvoidDrainingNames-3]
	_ = x[DrainingNamesMigration-4]
	_ = x[TraceIDDoesntImplyStructuredRecording-5]
	_ = x[AlterSystemTableStatisticsAddAvgSizeCol-6]
	_ = x[MVCCAddSSTable-7]
	_ = x[InsertPublicSchemaNamespaceEntryOnRestore-8]
	_ = x[UnsplitRangesInAsyncGCJobs-9]
	_ = x[ValidateGrantOption-10]
	_ = x[PebbleFormatBlockPropertyCollector-11]
	_ = x[ProbeRequest-12]
	_ = x[SelectRPCsTakeTracingInfoInband-13]
	_ = x[PreSeedTenantSpanConfigs-14]
	_ = x[SeedTenantSpanConfigs-15]
	_ = x[PublicSchemasWithDescriptors-16]
	_ = x[EnsureSpanConfigReconciliation-17]
	_ = x[EnsureSpanConfigSubscription-18]
	_ = x[EnableSpanConfigStore-19]
	_ = x[ScanWholeRows-20]
	_ = x[SCRAMAuthentication-21]
	_ = x[UnsafeLossOfQuorumRecoveryRangeLog-22]
	_ = x[AlterSystemProtectedTimestampAddColumn-23]
	_ = x[EnableProtectedTimestampsForTenant-24]
	_ = x[DeleteCommentsWithDroppedIndexes-25]
	_ = x[RemoveIncompatibleDatabasePrivileges-26]
	_ = x[AddRaftAppliedIndexTermMigration-27]
	_ = x[PostAddRaftAppliedIndexTermMigration-28]
	_ = x[DontProposeWriteTimestampForLeaseTransfers-29]
	_ = x[EnablePebbleFormatVersionBlockProperties-30]
	_ = x[DisableSystemConfigGossipTrigger-31]
	_ = x[MVCCIndexBackfiller-32]
	_ = x[EnableLeaseHolderRemoval-33]
	_ = x[BackupResolutionInJob-34]
	_ = x[LooselyCoupledRaftLogTruncation-35]
	_ = x[ChangefeedIdleness-36]
	_ = x[BackupDoesNotOverwriteLatestAndCheckpoint-37]
	_ = x[EnableDeclarativeSchemaChanger-38]
	_ = x[RowLevelTTL-39]
	_ = x[PebbleFormatSplitUserKeysMarked-40]
	_ = x[IncrementalBackupSubdir-41]
	_ = x[DateStyleIntervalStyleCastRewrite-42]
	_ = x[EnableNewStoreRebalancer-43]
	_ = x[ClusterLocksVirtualTable-44]
	_ = x[AutoStatsTableSettings-45]
	_ = x[ForecastStats-46]
	_ = x[SuperRegions-47]
	_ = x[EnableNewChangefeedOptions-48]
	_ = x[SpanCountTable-49]
	_ = x[PreSeedSpanCountTable-50]
	_ = x[SeedSpanCountTable-51]
	_ = x[V22_1-52]
	_ = x[Start22_2-53]
	_ = x[LocalTimestamps-54]
	_ = x[EnsurePebbleFormatVersionRangeKeys-55]
	_ = x[EnablePebbleFormatVersionRangeKeys-56]
	_ = x[TrigramInvertedIndexes-57]
	_ = x[RemoveGrantPrivilege-58]
	_ = x[MVCCRangeTombstones-59]
	_ = x[UpgradeSequenceToBeReferencedByID-60]
	_ = x[SampledStmtDiagReqs-61]
	_ = x[AddSSTableTombstones-62]
	_ = x[SystemPrivilegesTable-63]
	_ = x[EnablePredicateProjectionChangefeed-64]
	_ = x[AlterSystemSQLInstancesAddLocality-65]
}

const _Key_name = "V21_2Start22_1TargetBytesAvoidExcessAvoidDrainingNamesDrainingNamesMigrationTraceIDDoesntImplyStructuredRecordingAlterSystemTableStatisticsAddAvgSizeColMVCCAddSSTableInsertPublicSchemaNamespaceEntryOnRestoreUnsplitRangesInAsyncGCJobsValidateGrantOptionPebbleFormatBlockPropertyCollectorProbeRequestSelectRPCsTakeTracingInfoInbandPreSeedTenantSpanConfigsSeedTenantSpanConfigsPublicSchemasWithDescriptorsEnsureSpanConfigReconciliationEnsureSpanConfigSubscriptionEnableSpanConfigStoreScanWholeRowsSCRAMAuthenticationUnsafeLossOfQuorumRecoveryRangeLogAlterSystemProtectedTimestampAddColumnEnableProtectedTimestampsForTenantDeleteCommentsWithDroppedIndexesRemoveIncompatibleDatabasePrivilegesAddRaftAppliedIndexTermMigrationPostAddRaftAppliedIndexTermMigrationDontProposeWriteTimestampForLeaseTransfersEnablePebbleFormatVersionBlockPropertiesDisableSystemConfigGossipTriggerMVCCIndexBackfillerEnableLeaseHolderRemovalBackupResolutionInJobLooselyCoupledRaftLogTruncationChangefeedIdlenessBackupDoesNotOverwriteLatestAndCheckpointEnableDeclarativeSchemaChangerRowLevelTTLPebbleFormatSplitUserKeysMarkedIncrementalBackupSubdirDateStyleIntervalStyleCastRewriteEnableNewStoreRebalancerClusterLocksVirtualTableAutoStatsTableSettingsForecastStatsSuperRegionsEnableNewChangefeedOptionsSpanCountTablePreSeedSpanCountTableSeedSpanCountTableV22_1Start22_2LocalTimestampsEnsurePebbleFormatVersionRangeKeysEnablePebbleFormatVersionRangeKeysTrigramInvertedIndexesRemoveGrantPrivilegeMVCCRangeTombstonesUpgradeSequenceToBeReferencedByIDSampledStmtDiagReqsAddSSTableTombstonesSystemPrivilegesTableEnablePredicateProjectionChangefeedAlterSystemSQLInstancesAddLocality"

var _Key_index = [...]uint16{0, 5, 14, 36, 54, 76, 113, 152, 166, 207, 233, 252, 286, 298, 329, 353, 374, 402, 432, 460, 481, 494, 513, 547, 585, 619, 651, 687, 719, 755, 797, 837, 869, 888, 912, 933, 964, 982, 1023, 1053, 1064, 1095, 1118, 1151, 1175, 1199, 1221, 1234, 1246, 1272, 1286, 1307, 1325, 1330, 1339, 1354, 1388, 1422, 1444, 1464, 1483, 1516, 1535, 1555, 1576, 1611, 1645}

func (i Key) String() string {
	if i < 0 || i >= Key(len(_Key_index)-1) {
		return "Key(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _Key_name[_Key_index[i]:_Key_index[i+1]]
}
