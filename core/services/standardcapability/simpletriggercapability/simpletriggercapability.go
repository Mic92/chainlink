package simpletriggercapability

import (
	"context"

	"github.com/hashicorp/go-plugin"

	"github.com/smartcontractkit/chainlink-common/pkg/capabilities"
	"github.com/smartcontractkit/chainlink-common/pkg/loop"
	"github.com/smartcontractkit/chainlink-common/pkg/types/core"
)

const (
	loggerName = "PluginStandardCapability"
)

func main() {
	s := loop.MustNewStartedServer(loggerName)
	defer s.Stop()

	stopCh := make(chan struct{})
	defer close(stopCh)

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: loop.StandardTriggerCapabilityHandshakeConfig(),
		Plugins: map[string]plugin.Plugin{
			loop.PluginStandardTriggerCapabilityName: &loop.StandardTriggerCapabilityLoop{
				PluginServer: &CustomTriggerCapabilityService{},
				BrokerConfig: loop.BrokerConfig{Logger: s.Logger, StopCh: stopCh, GRPCOpts: s.GRPCOpts},
			},
		},
		GRPCServer: s.GRPCOpts.NewServer,
	})
}

type CustomTriggerCapabilityService struct {
	telemetryService core.TelemetryService
	store            core.KeyValueStore
}

func (c CustomTriggerCapabilityService) Start(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (c CustomTriggerCapabilityService) Close() error {
	//TODO implement me
	panic("implement me")
}

func (c CustomTriggerCapabilityService) Ready() error {
	//TODO implement me
	panic("implement me")
}

func (c CustomTriggerCapabilityService) HealthReport() map[string]error {
	//TODO implement me
	panic("implement me")
}

func (c CustomTriggerCapabilityService) Name() string {
	//TODO implement me
	panic("implement me")
}

func (c CustomTriggerCapabilityService) Info(ctx context.Context) (capabilities.CapabilityInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (c CustomTriggerCapabilityService) RegisterTrigger(ctx context.Context, request capabilities.CapabilityRequest) (<-chan capabilities.CapabilityResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c CustomTriggerCapabilityService) UnregisterTrigger(ctx context.Context, request capabilities.CapabilityRequest) error {
	//TODO implement me
	panic("implement me")
}

func (c CustomTriggerCapabilityService) Initialise(ctx context.Context, config string, telemetryService core.TelemetryService, store core.KeyValueStore) error {
	//TODO implement me
	panic("implement me")
}
