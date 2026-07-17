package storage

import deliverymodel "github.com/lincyaw/ag/sdk/storage/internal/deliverymodel"

var (
	validateNewDelivery    = deliverymodel.ValidateNew
	sameDeliveryIdentity   = deliverymodel.SameIdentity
	compareDeliveries      = deliverymodel.Compare
	deliveryPartition      = deliverymodel.Partition
	validateLoadedDelivery = deliverymodel.ValidateLoaded
)
