package storage

import trajectorymodel "github.com/lincyaw/ag/sdk/storage/internal/trajectorymodel"

var (
	validateNewTrajectory           = trajectorymodel.ValidateNewTrajectory
	validateTrajectoryParent        = trajectorymodel.ValidateTrajectoryParent
	prepareTrajectoryEntries        = trajectorymodel.PrepareTrajectoryEntries
	trajectoryMetadata              = trajectorymodel.TrajectoryMetadata
	summarizeTrajectory             = trajectorymodel.SummarizeTrajectory
	findLatestInBranch              = trajectorymodel.FindLatestInBranch
	latestEntry                     = trajectorymodel.LatestEntry
	latestCheckpointAfterAppend     = trajectorymodel.LatestCheckpointAfterAppend
	cloneTrajectory                 = trajectorymodel.CloneTrajectory
	normalizeTrajectory             = trajectorymodel.NormalizeTrajectory
	validateLoadedTrajectory        = trajectorymodel.ValidateLoadedTrajectory
	cloneTrajectoryEnvironment      = trajectorymodel.CloneTrajectoryEnvironment
	cloneTrajectoryEntry            = trajectorymodel.CloneTrajectoryEntry
	cloneTrajectoryExecution        = trajectorymodel.CloneTrajectoryExecution
	bindTrajectoryExecutionEntries  = trajectorymodel.BindTrajectoryExecutionEntries
	validateTrajectoryExecution     = trajectorymodel.ValidateTrajectoryExecution
	prepareTrajectoryExecutionStart = trajectorymodel.PrepareTrajectoryExecutionStart
	claimTrajectoryExecution        = trajectorymodel.ClaimTrajectoryExecution
	renewTrajectoryExecution        = trajectorymodel.RenewTrajectoryExecution
	commitTrajectoryExecution       = trajectorymodel.CommitTrajectoryExecution
	cancelTrajectoryExecution       = trajectorymodel.CancelTrajectoryExecution
	normalizedMutationTime          = trajectorymodel.NormalizeMutationTime
	validateTrajectoryKind          = trajectorymodel.ValidateTrajectoryKind
	validateTrajectoryEntryFields   = trajectorymodel.ValidateTrajectoryEntryFields
)
