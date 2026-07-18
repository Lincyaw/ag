package bootstrap

import (
	"context"

	"github.com/lincyaw/ag/sdk"
)

type EventSink interface {
	Observe(context.Context, sdk.Event)
}

func eventObserver(sink EventSink) func(context.Context, sdk.Event) {
	if sink == nil {
		return nil
	}
	return sink.Observe
}
