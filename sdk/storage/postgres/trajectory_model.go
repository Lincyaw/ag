package postgres

import trajectorymodel "github.com/lincyaw/ag/sdk/storage/internal/trajectorymodel"

var (
	validateNewTrajectory           = trajectorymodel.ValidateNewTrajectory
	prepareNewTrajectory            = trajectorymodel.PrepareNewTrajectory
	prepareNewTrajectoryFork        = trajectorymodel.PrepareNewTrajectoryFork
	prepareTrajectoryEntries        = trajectorymodel.PrepareTrajectoryEntries
	trajectoryMetadata              = trajectorymodel.TrajectoryMetadata
	summarizeTrajectory             = trajectorymodel.SummarizeTrajectory
	projectTrajectoryBranch         = trajectorymodel.ProjectTrajectoryBranch
	findLatestInBranch              = trajectorymodel.FindLatestInBranch
	findEntryOnBranch               = trajectorymodel.FindEntryOnBranch
	resolveBranch                   = trajectorymodel.ResolveBranch
	latestEntry                     = trajectorymodel.LatestEntry
	latestCheckpointAfterAppend     = trajectorymodel.LatestCheckpointAfterAppend
	normalizeTrajectory             = trajectorymodel.NormalizeTrajectory
	bindTrajectoryExecutionEntries  = trajectorymodel.BindTrajectoryExecutionEntries
	validateTrajectoryExecution     = trajectorymodel.ValidateTrajectoryExecution
	prepareTrajectoryExecutionStart = trajectorymodel.PrepareTrajectoryExecutionStart
	claimTrajectoryExecution        = trajectorymodel.ClaimTrajectoryExecution
	renewTrajectoryExecution        = trajectorymodel.RenewTrajectoryExecution
	commitTrajectoryExecution       = trajectorymodel.CommitTrajectoryExecution
	cancelTrajectoryExecution       = trajectorymodel.CancelTrajectoryExecution
	normalizedMutationTime          = trajectorymodel.NormalizeMutationTime
	validateTrajectoryKind          = trajectorymodel.ValidateTrajectoryKind
)
