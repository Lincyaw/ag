package storage

import contextinjectionmodel "github.com/lincyaw/ag/sdk/storage/internal/contextinjectionmodel"

var (
	prepareContextInjections     = contextinjectionmodel.PrepareBatch
	sameContextInjectionIdentity = contextinjectionmodel.SameIdentity
	validateLoadedContext        = contextinjectionmodel.ValidateLoaded
	validateLoadedContextRecord  = contextinjectionmodel.ValidateLoadedRecord
	validateContextQuery         = contextinjectionmodel.ValidateQuery
	contextMatchesQuery          = contextinjectionmodel.MatchesQuery
	sortContextRecords           = contextinjectionmodel.SortRecords
)
