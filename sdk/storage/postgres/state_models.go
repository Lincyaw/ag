package postgres

import (
	deliverymodel "github.com/lincyaw/ag/sdk/storage/internal/deliverymodel"
	operationmodel "github.com/lincyaw/ag/sdk/storage/internal/operationmodel"
)

var (
	validateNewOperationRecord  = operationmodel.ValidateNewRecord
	validateOperationTransition = operationmodel.ValidateTransition
	cloneOperationRecord        = operationmodel.CloneRecord
	sameOperationSubmission     = operationmodel.SameSubmission

	validateNewDelivery    = deliverymodel.ValidateNew
	sameDeliveryIdentity   = deliverymodel.SameIdentity
	deliveryPartition      = deliverymodel.Partition
	validateLoadedDelivery = deliverymodel.ValidateLoaded
)
