package main

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/go-plugin"

	"github.com/smartcontractkit/chainlink-common/pkg/capabilities"
	"github.com/smartcontractkit/chainlink-common/pkg/loop"
	"github.com/smartcontractkit/chainlink-common/pkg/values"
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
		HandshakeConfig: loop.StandardCallbackCapabilityHandshakeConfig(),
		Plugins: map[string]plugin.Plugin{
			loop.PluginStandardCallbackCapabilityName: &loop.StandardCallbackCapabilityLoop{
				PluginServer: CustomStandardCapabilityService{},
				BrokerConfig: loop.BrokerConfig{Logger: s.Logger, StopCh: stopCh, GRPCOpts: s.GRPCOpts},
			},
		},
		GRPCServer: s.GRPCOpts.NewServer,
	})
}

type CustomStandardCapabilityService struct {
}

func (c CustomStandardCapabilityService) Info(ctx context.Context) (capabilities.CapabilityInfo, error) {
	return capabilities.CapabilityInfo{
		ID:             "3",
		CapabilityType: capabilities.CapabilityTypeAction,
		Description:    "",
		Version:        "",
		DON:            nil,
	}, nil
}

func (c CustomStandardCapabilityService) RegisterToWorkflow(ctx context.Context, request capabilities.RegisterToWorkflowRequest) error {
	return nil
}

func (c CustomStandardCapabilityService) UnregisterFromWorkflow(ctx context.Context, request capabilities.UnregisterFromWorkflowRequest) error {
	return nil
}

func (c CustomStandardCapabilityService) Execute(ctx context.Context, request capabilities.CapabilityRequest) (<-chan capabilities.CapabilityResponse, error) {
	result := make(chan capabilities.CapabilityResponse, 100)

	go func() {
		defer close(result)
		for i := 0; i < 10; i++ {
			value, err := values.Wrap(fmt.Sprintf("Hello World! %d", i))
			if err != nil {
				// log
				return
			}

			result <- capabilities.CapabilityResponse{
				Value: value,
				Err:   nil,
			}
			time.Sleep(1 * time.Second)
		}
	}()

	return result, nil
}

func (c CustomStandardCapabilityService) Initialise(ctx context.Context, config string, errorLogID uint32, pipelineRunnerID uint32, telemetryID uint32, capRegistryID uint32, keyValueStoreID uint32, relayerSetID uint32) error {
	return nil
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
