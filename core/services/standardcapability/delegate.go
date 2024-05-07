package standardcapability

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/pelletier/go-toml"
	"github.com/pkg/errors"

	"github.com/smartcontractkit/chainlink-common/pkg/capabilities"
	"github.com/smartcontractkit/chainlink-common/pkg/loop"
	"github.com/smartcontractkit/chainlink-common/pkg/types/core"
	"github.com/smartcontractkit/chainlink/v2/core/logger"
	"github.com/smartcontractkit/chainlink/v2/core/services/job"
	"github.com/smartcontractkit/chainlink/v2/plugins"
)

type Delegate struct {
	logger   logger.Logger
	registry core.CapabilitiesRegistry
	cfg      plugins.RegistrarConfig
}

func NewDelegate(logger logger.Logger, registry core.CapabilitiesRegistry,
	cfg plugins.RegistrarConfig) *Delegate {
	return &Delegate{logger: logger, registry: registry, cfg: cfg}
}

func (d Delegate) JobType() job.Type {
	return job.StandardCapability
}

func (d Delegate) BeforeJobCreated(job job.Job) {}

func (d Delegate) ServicesForSpec(ctx context.Context, job job.Job) ([]job.ServiceCtx, error) {

	log := d.logger.Named("StandardCapability").Named("name from config")
	var envVars []string
	cmdName := "/Users/matthewpendrey/Projects/chainlink/core/services/standardcapability/simplestandardcapability/simplestandardcapability" // get a better version of this from the test code

	cmdFn, opts, err := d.cfg.RegisterLOOP(plugins.CmdConfig{
		ID:  log.Name(),
		Cmd: cmdName,
		Env: envVars,
	})

	if err != nil {
		return nil, fmt.Errorf("error registering loop: %v", err)
	}

	scs := loop.NewStandardCallbackCapability(log, opts, cmdFn)

	err = scs.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting standard capability service: %v", err)
	}

	err = scs.WaitCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("error waiting for standard capability service to start: %v", err)
	}

	err = scs.Service.Initialise(ctx, "", 0, 0, 0, 0, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("error initialising standard capability service: %v", err)
	}

	resultCh, err := scs.Service.Execute(ctx, capabilities.CapabilityRequest{})
	if err != nil {
		return nil, fmt.Errorf("error creating standard capability: %v", err)
	}

	for resp := range resultCh {
		fmt.Printf("Got response from standard capability: %v\n", resp.Value)
	}

	err = d.registry.Add(ctx, scs.Service)
	if err != nil {
		return nil, fmt.Errorf("error adding standard callback capability to registry: %w", err)

	}

	//here - test this

	//fmt.Printf("Created standard capability with id %d\n", capabilityID)

	//get all this working and test methods of the capability, then wrap and register, after that will rename the job
	//fields for the capability type

	//d.registry.Add(ctx, capabilityID)

	//here - now that the job configuration has been added, shoujld be able to configure and run a job

	// Create a client to the capability, and register it with the registry, where does the proxying happen?

	/*
		d.registry.Add(ctx, capability)

		median := loop.NewMedianService(lggr, telem, cmdFn, medianProvider, dataSource, juelsPerFeeCoinSource, errorLog)
		argsNoPlugin.ReportingPluginFactory = median
		srvs = append(srvs, median)

		// see this-> if cmdName := env.MedianPlugin.Cmd.Get(); cmdName != "" {

		//1 start up the binary assume it's a loop binary

		// wait for startup by listening to the loop registry? then wire up dependencies by passing over service ids to the loop binary? or is there a different way
		// that services are passed to the loop binary?

		// register the capability with the capability registry

		// return a service context  to enable shutdown

		// So approach is to create a bare bones impl for the above, then figure out how to configure the core to test it

		// After all this is done and tested, need to figure out how the binary will be deployed and loaded in practise.

		d.registry.

	*/
	return nil, nil
}

func (d Delegate) AfterJobCreated(job job.Job) {}

func (d Delegate) BeforeJobDeleted(job job.Job) {}

func (d Delegate) OnDeleteJob(ctx context.Context, jb job.Job) error { return nil }

func ValidatedStandardCapabilitySpec(tomlString string) (job.Job, error) {
	var jb = job.Job{ExternalJobID: uuid.New()}

	tree, err := toml.Load(tomlString)
	if err != nil {
		return jb, errors.Wrap(err, "toml error on load standard capability")
	}

	err = tree.Unmarshal(&jb)
	if err != nil {
		return jb, errors.Wrap(err, "toml unmarshal error on standard capability spec")
	}

	var spec job.StandardCapabilitySpec
	err = tree.Unmarshal(&spec)
	if err != nil {
		return jb, errors.Wrap(err, "toml unmarshal error on standard capability job")
	}

	jb.StandardCapabilitySpec = &spec
	if jb.Type != job.StandardCapability {
		return jb, errors.Errorf("standard capability unsupported job type %s", jb.Type)
	}

	return jb, nil
}
