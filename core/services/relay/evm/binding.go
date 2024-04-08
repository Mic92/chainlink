package evm

import (
	"context"

	commontypes "github.com/smartcontractkit/chainlink-common/pkg/types"
	"github.com/smartcontractkit/chainlink-common/pkg/types/query"
)

type readBinding interface {
	GetLatestValue(ctx context.Context, params, returnVal any) error
	QueryOne(ctx context.Context, queryFilter query.Filter, limitAndSort query.LimitAndSort, sequenceDataType any) ([]commontypes.Sequence, error)
	Bind(ctx context.Context, binding commontypes.BoundContract) error
	SetCodec(codec commontypes.RemoteCodec)
	Register(ctx context.Context) error
	Unregister(ctx context.Context) error
}
