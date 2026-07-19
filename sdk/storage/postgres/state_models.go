package postgres

import (
	contextinjectionmodel "github.com/lincyaw/ag/sdk/storage/internal/contextinjectionmodel"
	deliverymodel "github.com/lincyaw/ag/sdk/storage/internal/deliverymodel"
	operationmodel "github.com/lincyaw/ag/sdk/storage/internal/operationmodel"
)

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
	cloneOperationRecord           = operationmodel.CloneRecord
	sameOperationSubmission        = operationmodel.SameSubmission
	validateLoadedOperationRecord  = operationmodel.ValidateLoadedRecord

	prepareContextInjections     = contextinjectionmodel.PrepareBatch
	sameContextInjectionIdentity = contextinjectionmodel.SameIdentity
	validateLoadedContextRecord  = contextinjectionmodel.ValidateLoadedRecord
	validateContextQuery         = contextinjectionmodel.ValidateQuery

	prepareNewDeliveries          = deliverymodel.PrepareNewBatch
	sameDeliveryIdentity          = deliverymodel.SameIdentity
	leaseDelivery                 = deliverymodel.Lease
	finishDeliveryLease           = deliverymodel.FinishLease
	validateDeliveryLeaseDuration = deliverymodel.ValidateLeaseDuration
	normalizeDeliveryMutationTime = deliverymodel.NormalizeMutationTime
	validateLoadedDelivery        = deliverymodel.ValidateLoaded
)
