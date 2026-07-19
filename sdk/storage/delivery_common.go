package storage

import deliverymodel "github.com/lincyaw/ag/sdk/storage/internal/deliverymodel"

var (
	prepareNewDeliveries          = deliverymodel.PrepareNewBatch
	sameDeliveryIdentity          = deliverymodel.SameIdentity
	compareDeliveries             = deliverymodel.Compare
	deliveryPartition             = deliverymodel.Partition
	deliveryAvailable             = deliverymodel.LeaseAvailable
	leaseDelivery                 = deliverymodel.Lease
	finishDeliveryLease           = deliverymodel.FinishLease
	validateDeliveryLeaseDuration = deliverymodel.ValidateLeaseDuration
	normalizeDeliveryMutationTime = deliverymodel.NormalizeMutationTime
	validateLoadedDelivery        = deliverymodel.ValidateLoaded
)
