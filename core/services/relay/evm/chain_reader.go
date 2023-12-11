package evm

import (
	"context"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	commonservices "github.com/smartcontractkit/chainlink-common/pkg/services"
	commontypes "github.com/smartcontractkit/chainlink-common/pkg/types"

	evmclient "github.com/smartcontractkit/chainlink/v2/core/chains/evm/client"
	evmtypes "github.com/smartcontractkit/chainlink/v2/core/chains/evm/types"
	"github.com/smartcontractkit/chainlink/v2/core/chains/legacyevm"

	"github.com/smartcontractkit/chainlink/v2/core/chains/evm/logpoller"
	"github.com/smartcontractkit/chainlink/v2/core/logger"
	"github.com/smartcontractkit/chainlink/v2/core/services"
	"github.com/smartcontractkit/chainlink/v2/core/services/relay/evm/types"
)

type ChainReaderService interface {
	services.ServiceCtx
	commontypes.ChainReader
}

type chainReader struct {
	lggr     logger.Logger
	lp       logpoller.LogPoller
	codec    commontypes.RemoteCodec
	client   evmclient.Client
	bindings Bindings
	commonservices.StateMachine
}

// NewChainReaderService is a constructor for ChainReader, returns nil if there is any error
func NewChainReaderService(lggr logger.Logger, lp logpoller.LogPoller, b Bindings, chain legacyevm.Chain, config types.ChainReaderConfig) (ChainReaderService, error) {
	parsed := &parsedTypes{
		encoderDefs: map[string]*codecEntry{},
		decoderDefs: map[string]*codecEntry{},
	}

	if err := addTypes(config.ChainContractReaders, b, parsed); err != nil {
		return nil, err
	}

	c, err := parsed.toCodec()

	return &chainReader{
		lggr:     lggr.Named("ChainReader"),
		lp:       lp,
		codec:    c,
		client:   chain.Client(),
		bindings: b,
	}, err
}

func (cr *chainReader) Name() string { return cr.lggr.Name() }

var _ commontypes.ContractTypeProvider = &chainReader{}

func (cr *chainReader) GetLatestValue(ctx context.Context, bc commontypes.BoundContract, method string, params any, returnVal any) error {
	b, err := cr.bindings.getBinding(bc.Name, method)
	if err != nil {
		return err
	}

	if b.evt == nil {
		return cr.getLatestValueFromContract(ctx, bc, method, params, returnVal)
	}

	return cr.getLatestValueFromLogPoller(ctx, bc, method, *b.evt, returnVal)
}

func (cr *chainReader) getLatestValueFromLogPoller(ctx context.Context, bc commontypes.BoundContract, method string, hash common.Hash, returnVal any) error {
	contractAddr := common.HexToAddress(bc.Address)
	log, err := cr.lp.LatestLogByEventSigWithConfs(hash, contractAddr, logpoller.Finalized)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return fmt.Errorf("%w: %w", commontypes.ErrNotFound, err)
		}
		return fmt.Errorf("%w: %w", commontypes.ErrInternal, err)
	}
	return cr.codec.Decode(ctx, log.Data, returnVal, wrapItemType(bc.Name, method, false))
}

func (cr *chainReader) getLatestValueFromContract(ctx context.Context, bc commontypes.BoundContract, method string, params any, returnVal any) error {
	data, err := cr.codec.Encode(ctx, params, wrapItemType(bc.Name, method, true))
	if err != nil {
		return err
	}

	address := common.HexToAddress(bc.Address)
	callMsg := ethereum.CallMsg{
		To:   &address,
		From: address,
		Data: data,
	}

	output, err := cr.client.CallContract(ctx, callMsg, nil)

	if err != nil {
		return err
	}

	return cr.codec.Decode(ctx, output, returnVal, wrapItemType(bc.Name, method, false))
}

func (cr *chainReader) Start(_ context.Context) error {
	return cr.StartOnce("ChainReader", func() error {
		for contractName, contractEvents := range cr.bindings {
			for eventName, b := range contractEvents {
				if b.evt == nil {
					continue
				}

				if err := cr.lp.RegisterFilter(logpoller.Filter{
					Name:      wrapItemType(contractName, eventName, false),
					EventSigs: evmtypes.HashArray{*b.evt},
					Addresses: evmtypes.AddressArray{b.addr},
				}); err != nil {
					return fmt.Errorf("%w: %w", commontypes.ErrInternal, err)
				}
			}
		}
		return nil
	})
}
func (cr *chainReader) Close() error {
	return cr.StopOnce("ChainReader", func() error {
		for contractName, contractEvents := range cr.bindings {
			for eventName := range contractEvents {
				if err := cr.lp.UnregisterFilter(wrapItemType(contractName, eventName, false)); err != nil {
					return fmt.Errorf("%w: %w", commontypes.ErrInternal, err)
				}
			}
		}
		return nil
	})
}

func (cr *chainReader) Ready() error { return nil }
func (cr *chainReader) HealthReport() map[string]error {
	return map[string]error{cr.Name(): nil}
}

func (cr *chainReader) CreateContractType(contractName, methodName string, forEncoding bool) (any, error) {
	return cr.codec.CreateType(wrapItemType(contractName, methodName, forEncoding), forEncoding)
}

func addEventTypes(contractName, methodName string, b Bindings, contractABI abi.ABI, chainReaderDefinition types.ChainReaderDefinition, parsed *parsedTypes) error {
	event, methodExists := contractABI.Events[chainReaderDefinition.ChainSpecificName]
	if !methodExists {
		return fmt.Errorf("%w: method %s doesn't exist", commontypes.ErrInvalidConfig, chainReaderDefinition.ChainSpecificName)
	}

	if err := b.addEvent(contractName, methodName, event.ID); err != nil {
		return err
	}

	return addDecoderDef(contractName, methodName, event.Inputs, parsed, chainReaderDefinition)
}

func addMethods(
	contractName, methodName string, abi abi.ABI, chainReaderDefinition types.ChainReaderDefinition, parsed *parsedTypes) error {
	method, methodExists := abi.Methods[chainReaderDefinition.ChainSpecificName]
	if !methodExists {
		return fmt.Errorf("method: %q doesn't exist", chainReaderDefinition.ChainSpecificName)
	}

	if err := addEncoderDef(contractName, methodName, method, parsed, chainReaderDefinition); err != nil {
		return err
	}

	return addDecoderDef(contractName, methodName, method.Outputs, parsed, chainReaderDefinition)
}

func addEncoderDef(contractName, methodName string, method abi.Method, parsed *parsedTypes, chainReaderDefinition types.ChainReaderDefinition) error {
	// ABI.Pack prepends the method.ID to the encodings, we'll need the encoder to do the same.
	input := &codecEntry{Args: method.Inputs, encodingPrefix: method.ID}

	if err := input.Init(); err != nil {
		return err
	}

	inputMod, err := chainReaderDefinition.InputModifications.ToModifier(evmDecoderHooks...)
	if err != nil {
		return err
	}
	input.mod = inputMod
	parsed.encoderDefs[wrapItemType(contractName, methodName, true)] = input
	return nil
}

func addDecoderDef(contractName, methodName string, outputs abi.Arguments, parsed *parsedTypes, def types.ChainReaderDefinition) error {
	output := &codecEntry{Args: outputs}
	mod, err := def.OutputModifications.ToModifier(evmDecoderHooks...)
	if err != nil {
		return err
	}
	output.mod = mod
	parsed.decoderDefs[wrapItemType(contractName, methodName, false)] = output
	return output.Init()
}

func addTypes(chainContractReaders map[string]types.ChainContractReader, b Bindings, parsed *parsedTypes) error {
	for contractName, chainContractReader := range chainContractReaders {
		contractAbi, err := abi.JSON(strings.NewReader(chainContractReader.ContractABI))
		if err != nil {
			return err
		}

		for typeName, chainReaderDefinition := range chainContractReader.ChainReaderDefinitions {
			switch chainReaderDefinition.ReadType {
			case types.Method:
				err = addMethods(contractName, typeName, contractAbi, chainReaderDefinition, parsed)
			case types.Event:
				err = addEventTypes(contractName, typeName, b, contractAbi, chainReaderDefinition, parsed)
			default:
				return fmt.Errorf(
					"%w: invalid chain reader definition read type: %d",
					commontypes.ErrInvalidConfig,
					chainReaderDefinition.ReadType)
			}

			if err != nil {
				return err
			}
		}
	}

	return nil
}

func wrapItemType(contractName, methodName string, isParams bool) string {
	if isParams {
		return fmt.Sprintf("params.%s.%s", contractName, methodName)
	}
	return fmt.Sprintf("return.%s.%s", contractName, methodName)
}
