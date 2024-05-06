package main

import (
	"context"

	"github.com/hashicorp/go-plugin"

	"github.com/smartcontractkit/chainlink-common/pkg/loop"
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
		HandshakeConfig: loop.StandardCapabilityServiceHandshakeConfig(),
		Plugins: map[string]plugin.Plugin{
			loop.PluginStandardCapabilityName: &loop.StandardCapabilityLoop{
				PluginServer: CustomStandardCapabilityService{},
				BrokerConfig: loop.BrokerConfig{Logger: s.Logger, StopCh: stopCh, GRPCOpts: s.GRPCOpts},
			},
		},
		GRPCServer: s.GRPCOpts.NewServer,
	})
}

type CustomStandardCapabilityService struct {
}

func (c CustomStandardCapabilityService) Start(ctx context.Context) error {
	return nil
}

func (c CustomStandardCapabilityService) Close() error {
	return nil
}

func (c CustomStandardCapabilityService) Ready() error {
	return nil
}

func (c CustomStandardCapabilityService) HealthReport() map[string]error {
	return map[string]error{}
}

func (c CustomStandardCapabilityService) Name() string {
	return "simplestandardcapability"
}

func (c CustomStandardCapabilityService) NewStandardCapability(ctx context.Context, config string, errorLogID uint32,
	pipelineRunnerID uint32, telemetryID uint32, capRegistryID uint32, keyValueStoreID uint32, relayerSetID uint32) (uint32, error) {

	// here would create the clients to the services and then run the capability service and return the service id
	// the part to instantiate the client services should be in common eventually

	return 2, nil
}
