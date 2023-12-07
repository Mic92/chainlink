package evm_test

import (
	"encoding/json"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	ocr2types "github.com/smartcontractkit/libocr/offchainreporting2plus/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commontypes "github.com/smartcontractkit/chainlink-common/pkg/types"
	. "github.com/smartcontractkit/chainlink-common/pkg/types/interfacetests" //nolint common practice to import test mods with .

	"github.com/smartcontractkit/chainlink/v2/core/internal/testutils"
	"github.com/smartcontractkit/chainlink/v2/core/services/relay/evm"
	"github.com/smartcontractkit/chainlink/v2/core/services/relay/evm/testfiles"
	"github.com/smartcontractkit/chainlink/v2/core/services/relay/evm/types"
)

func TestCodec(t *testing.T) {
	RunCodecInterfaceTests(t, &codecInterfaceTester{})
	anyN := 10
	t.Run("GetMaxEncodingSize delegates to GetMaxSize", func(t *testing.T) {
		codec := getCodec(t)

		actual, err := codec.GetMaxEncodingSize(testutils.Context(t), anyN, sizeItemType)
		assert.NoError(t, err)

		expected, err := evm.GetMaxSize(anyN, parseDefs(t)[sizeItemType])
		require.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("GetMaxDecodingSize delegates to GetMaxSize", func(t *testing.T) {
		codec := getCodec(t)

		actual, err := codec.GetMaxDecodingSize(testutils.Context(t), anyN, sizeItemType)
		assert.NoError(t, err)

		expected, err := evm.GetMaxSize(anyN, parseDefs(t)[sizeItemType])
		require.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
}

type codecInterfaceTester struct{}

func (it *codecInterfaceTester) Setup(_ *testing.T) {}

func (it *codecInterfaceTester) Teardown(_ *testing.T) {}

func (it *codecInterfaceTester) GetAccountBytes(i int) []byte {
	account := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2}
	account[i%32] += byte(i)
	account[(i+3)%32] += byte(i + 3)
	return account
}

func (it *codecInterfaceTester) EncodeFields(t *testing.T, request *EncodeRequest) []byte {
	if request.TestOn == TestItemType {
		return encodeFieldsOnItem(t, request)
	}

	return encodeFieldsOnSliceOrArray(t, request)
}

func (it *codecInterfaceTester) GetCodec(t *testing.T) commontypes.Codec {
	return getCodec(t)
}

func (it *codecInterfaceTester) IncludeArrayEncodingSizeEnforcement() bool {
	return true
}
func (it *codecInterfaceTester) Name() string {
	return "EVM"
}

func encodeFieldsOnItem(t *testing.T, request *EncodeRequest) ocr2types.Report {
	return packArgs(t, argsFromTestStruct(request.TestStructs[0]), parseDefs(t)[TestItemType], request)
}

func encodeFieldsOnSliceOrArray(t *testing.T, request *EncodeRequest) []byte {
	oargs := parseDefs(t)[request.TestOn]
	args := make([]any, 1)

	switch request.TestOn {
	case TestItemArray1Type:
		args[0] = [1]testfiles.TestStruct{toInternalType(request.TestStructs[0])}
	case TestItemArray2Type:
		args[0] = [2]testfiles.TestStruct{toInternalType(request.TestStructs[0]), toInternalType(request.TestStructs[1])}
	default:
		tmp := make([]testfiles.TestStruct, len(request.TestStructs))
		for i, ts := range request.TestStructs {
			tmp[i] = toInternalType(ts)
		}
		args[0] = tmp
	}

	return packArgs(t, args, oargs, request)
}

func packArgs(t *testing.T, allArgs []any, oargs abi.Arguments, request *EncodeRequest) []byte {
	// extra capacity in case we add an argument
	args := make(abi.Arguments, len(oargs), len(oargs)+1)
	copy(args, oargs)
	// decoding has extra field to decode
	if request.ExtraField {
		fakeType, err := abi.NewType("int32", "", []abi.ArgumentMarshaling{})
		require.NoError(t, err)
		args = append(args, abi.Argument{Name: "FakeField", Type: fakeType})
		allArgs = append(allArgs, 11)
	}

	if request.MissingField {
		args = args[1:]       //nolint we know it's non-zero len
		allArgs = allArgs[1:] //nolint we know it's non-zero len
	}

	bytes, err := args.Pack(allArgs...)
	require.NoError(t, err)
	return bytes
}

var inner = []abi.ArgumentMarshaling{
	{Name: "I", Type: "int64"},
	{Name: "S", Type: "string"},
}

var nested = []abi.ArgumentMarshaling{
	{Name: "FixedBytes", Type: "bytes2"},
	{Name: "Inner", Type: "tuple", Components: inner},
}

var ts = []abi.ArgumentMarshaling{
	{Name: "Field", Type: "int32"},
	{Name: "DifferentField", Type: "string"},
	{Name: "OracleId", Type: "uint8"},
	{Name: "OracleIds", Type: "uint8[32]"},
	{Name: "Account", Type: "bytes32"},
	{Name: "Accounts", Type: "bytes32[]"},
	{Name: "BigField", Type: "int192"},
	{Name: "NestedStruct", Type: "tuple", Components: nested},
}

const sizeItemType = "item for size"

var codecDefs = map[string][]abi.ArgumentMarshaling{
	TestItemType: ts,
	TestItemSliceType: {
		{Name: "", Type: "tuple[]", Components: ts},
	},
	TestItemArray1Type: {
		{Name: "", Type: "tuple[1]", Components: ts},
	},
	TestItemArray2Type: {
		{Name: "", Type: "tuple[2]", Components: ts},
	},
	sizeItemType: {
		{Name: "Stuff", Type: "int256[]"},
		{Name: "OtherStuff", Type: "int256"},
	},
}

func getCodec(t require.TestingT) commontypes.Codec {
	codecConfig := types.CodecConfig{
		ChainCodecConfigs: map[string]types.ChainCodedConfig{},
	}

	for k, v := range codecDefs {
		defBytes, err := json.Marshal(v)
		require.NoError(t, err)
		entry := codecConfig.ChainCodecConfigs[k]
		entry.TypeAbi = string(defBytes)
		codecConfig.ChainCodecConfigs[k] = entry
	}

	codec, err := evm.NewCodec(codecConfig)
	require.NoError(t, err)
	return codec
}

func parseDefs(t *testing.T) map[string]abi.Arguments {
	bytes, err := json.Marshal(codecDefs)
	require.NoError(t, err)
	var results map[string]abi.Arguments
	require.NoError(t, json.Unmarshal(bytes, &results))
	return results
}