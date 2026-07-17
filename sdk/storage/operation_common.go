package storage

import operationmodel "github.com/lincyaw/ag/sdk/storage/internal/operationmodel"

var (
	validateNewOperationRecord    = operationmodel.ValidateNewRecord
	validateOperationTransition   = operationmodel.ValidateTransition
	operationIdempotencyIndex     = operationmodel.IdempotencyIndex
	cloneOperationRecord          = operationmodel.CloneRecord
	sameOperationSubmission       = operationmodel.SameSubmission
	validateLoadedOperationRecord = operationmodel.ValidateLoadedRecord
)
