package duckdb

import (
	deliverymodel "github.com/lincyaw/ag/sdk/storage/internal/deliverymodel"
	operationmodel "github.com/lincyaw/ag/sdk/storage/internal/operationmodel"
	trajectorymodel "github.com/lincyaw/ag/sdk/storage/internal/trajectorymodel"
)

var (
	validateNewTrajectory           = trajectorymodel.ValidateNewTrajectory
	prepareNewTrajectory            = trajectorymodel.PrepareNewTrajectory
	prepareNewTrajectoryFork        = trajectorymodel.PrepareNewTrajectoryFork
	prepareTrajectoryEntries        = trajectorymodel.PrepareTrajectoryEntries
	trajectoryMetadata              = trajectorymodel.TrajectoryMetadata
	findLatestInBranch              = trajectorymodel.FindLatestInBranch
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

	prepareNewDeliveries          = deliverymodel.PrepareNewBatch
	sameDeliveryIdentity          = deliverymodel.SameIdentity
	leaseDelivery                 = deliverymodel.Lease
	finishDeliveryLease           = deliverymodel.FinishLease
	validateDeliveryLeaseDuration = deliverymodel.ValidateLeaseDuration
	normalizeDeliveryMutationTime = deliverymodel.NormalizeMutationTime
	validateLoadedDelivery        = deliverymodel.ValidateLoaded

	prepareNewOperationRecord      = operationmodel.PrepareNewRecord
	validateOperationClaim         = operationmodel.ValidateClaim
	validateOperationLeaseDuration = operationmodel.ValidateLeaseDuration
	validateOperationCompletion    = operationmodel.ValidateCompletionState
	normalizeOperationMutationTime = operationmodel.NormalizeMutationTime
	cancelOperation                = operationmodel.Cancel
	failOperation                  = operationmodel.Fail
	claimOperation                 = operationmodel.Claim
	renewOperation                 = operationmodel.Renew
	completeOperation              = operationmodel.Complete
	releaseOperation               = operationmodel.Release
	cloneOperationRecord           = operationmodel.CloneRecord
	sameOperationSubmission        = operationmodel.SameSubmission
	validateLoadedOperationRecord  = operationmodel.ValidateLoadedRecord
)
