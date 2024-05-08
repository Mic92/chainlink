package standardcapability

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/pelletier/go-toml"
	"github.com/pkg/errors"

	"github.com/smartcontractkit/chainlink-common/pkg/capabilities"
	"github.com/smartcontractkit/chainlink-common/pkg/loop"
	"github.com/smartcontractkit/chainlink-common/pkg/sqlutil"
	"github.com/smartcontractkit/chainlink-common/pkg/types/core"
	"github.com/smartcontractkit/chainlink/v2/core/logger"
	"github.com/smartcontractkit/chainlink/v2/core/services/job"
	"github.com/smartcontractkit/chainlink/v2/core/services/ocr2/plugins/generic"
	"github.com/smartcontractkit/chainlink/v2/core/services/telemetry"
	"github.com/smartcontractkit/chainlink/v2/plugins"
)

type Delegate struct {
	logger                logger.Logger
	ds                    sqlutil.DataSource
	registry              core.CapabilitiesRegistry
	cfg                   plugins.RegistrarConfig
	monitoringEndpointGen telemetry.MonitoringEndpointGenerator
}

func NewDelegate(logger logger.Logger, ds sqlutil.DataSource, registry core.CapabilitiesRegistry,
	cfg plugins.RegistrarConfig, monitoringEndpointGen telemetry.MonitoringEndpointGenerator) *Delegate {
	return &Delegate{logger: logger, ds: ds, registry: registry, cfg: cfg, monitoringEndpointGen: monitoringEndpointGen}
}

func (d Delegate) JobType() job.Type {
	return job.StandardCapability
}

func (d Delegate) BeforeJobCreated(job job.Job) {}

func (d Delegate) ServicesForSpec(ctx context.Context, jb job.Job) ([]job.ServiceCtx, error) {

	log := d.logger.Named("StandardCapability").Named("name from config")

	cmdName := jb.StandardCapabilitySpec.BinaryUrl

	// TEMP override
	cmdName = "/Users/matthewpendrey/Projects/chainlink/core/services/standardcapability/simplestandardcapability/simplestandardcapability" // get a better version of this from the test code

	cmdFn, opts, err := d.cfg.RegisterLOOP(plugins.CmdConfig{
		ID:  log.Name(),
		Cmd: cmdName,
		Env: nil,
	})

	if err != nil {
		return nil, fmt.Errorf("error registering loop: %v", err)
	}

	var capabilityLoop *loop.StandardCallbackCapabilityService

	// TEMP set this
	jb.StandardCapabilitySpec.CapabilityType = capabilities.CapabilityTypeAction.String()

	switch jb.StandardCapabilitySpec.CapabilityType {
	case capabilities.CapabilityTypeAction.String():
		fallthrough
	case capabilities.CapabilityTypeConsensus.String():
		fallthrough
	case capabilities.CapabilityTypeTarget.String():
		capabilityLoop = loop.NewStandardCallbackCapability(log, opts, cmdFn)
	case capabilities.CapabilityTypeTrigger.String():
		// TODO - impl the trigger capability loop
	default:
		return nil, fmt.Errorf("unsupported capability type: %s", jb.StandardCapabilitySpec.CapabilityType)
	}

	// TODO Move the below into service context

	err = capabilityLoop.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting standard capability service: %v", err)
	}

	err = capabilityLoop.WaitCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("error waiting for standard capability service to start: %v", err)
	}

	info, err := capabilityLoop.Service.Info(ctx)

	if err != nil {
		return nil, fmt.Errorf("error getting standard capability service info: %v", err)
	}

	// TODO here would validate the info matches that expected from the job spec and that the version, id and type match
	// those declared in the job spec
	fmt.Printf("Got info from standard capability: %v\n", info)

	// Not required?
	if info.CapabilityType.String() != jb.StandardCapabilitySpec.CapabilityType {
		return nil, fmt.Errorf("capability type mismatch: %s != %s", info.CapabilityType, jb.StandardCapabilitySpec.CapabilityType)
	}

	kvStore := job.NewKVStore(jb.ID, d.ds, log)
	telemetryService := generic.NewTelemetryAdapter(d.monitoringEndpointGen)

	err = capabilityLoop.Service.Initialise(ctx, jb.StandardCapabilitySpec.CapabilityConfig, telemetryService, kvStore)
	if err != nil {
		return nil, fmt.Errorf("error initialising standard capability service: %v", err)
	}

	// here - shonky test for now to check communication with the capability
	resultCh, err := capabilityLoop.Service.Execute(ctx, capabilities.CapabilityRequest{})
	if err != nil {
		return nil, fmt.Errorf("error creating standard capability: %v", err)
	}

	for resp := range resultCh {
		fmt.Printf("Got response from standard capability: %v\n", resp.Value)
	}
	// end of shonky test

	// TODO - move this into the capability -
	err = d.registry.Add(ctx, capabilityLoop.Service)
	if err != nil {
		return nil, fmt.Errorf("error adding standard callback capability to registry: %w", err)
	}

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

	// TODO other validation

	return jb, nil
}
