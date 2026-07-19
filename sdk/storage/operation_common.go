package storage

import operationmodel "github.com/lincyaw/ag/sdk/storage/internal/operationmodel"

var (
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
	operationRecoverableAt         = operationmodel.RecoverableAt
	operationIdempotencyIndex      = operationmodel.IdempotencyIndex
	cloneOperationRecord           = operationmodel.CloneRecord
	sameOperationSubmission        = operationmodel.SameSubmission
	validateLoadedOperationRecord  = operationmodel.ValidateLoadedRecord
)
