package codec

import (
	"github.com/squareup/pranadb/common"
	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
	"testing"
)

func TestJSONCodec(t *testing.T) {
	codec := &JSONCodec{}
	obj := map[string]interface{}{}
	obj["f2"] = "foo"
	obj["f3"] = float64(1.23)
	nested := map[string]interface{}{}
	obj["nested"] = nested
	nested["f1"] = "bar"
	buff, err := codec.Encode(obj)
	require.NoError(t, err)
	require.NotNil(t, buff)
	intf, err := codec.Decode(buff)
	require.NoError(t, err)
	require.NotNil(t, intf)
	obj2 := intf.(map[string]interface{}) //nolint:forcetypeassert
	assert.DeepEqual(t, obj, obj2)
}

func TestKafkaCodecFloat32BE(t *testing.T) {
	testKafkaCodecFloat32BE(t, float32(25.5), 25.5)
	testKafkaCodecFloat32BE(t, float32(0), 0)
	testKafkaCodecFloat32BE(t, float32(-123.23), -123.23)
	testKafkaCodecFloat32BE(t, "234.45", 234.45)
	testKafkaCodecFloat32BE(t, int64(234), 234)
	testKafkaCodecFloat32BE(t, int64(-234), -234)
	testKafkaCodecFloat32BE(t, int32(234), 234)
	testKafkaCodecFloat32BE(t, int32(-234), -234)
}

// TODO more tests

func testKafkaCodecFloat32BE(t *testing.T, val interface{}, expected float32) {
	t.Helper()
	codec := &KafkaCodec{encoding: common.KafkaEncodingFloat32BE}
	buff, err := codec.Encode(val)
	require.NoError(t, err)
	v, err := codec.Decode(buff)
	require.NoError(t, err)
	v2 := v.(float32) //nolint:forcetypeassert
	require.Equal(t, expected, v2)
}
