package kubemodel_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/proptest"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// This file is package kubemodel_test rather than kubemodel because it imports
// core/pkg/proptest, which imports kubemodel. An in-package test file would make
// that an import cycle. Everything asserted here is exported API, so the
// external test package costs nothing.

// Feature: gpu-inference-efficiency, Property 24: InferenceEngine round trip —
// for all valid InferenceEngine values, decoding the result of encoding a value
// produces a value equal to the original across every field, with floating-point
// fields compared bit-identically because the binary encoding is lossless.
func TestProperty24_InferenceEngineRoundTrip(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		want := proptest.GenInferenceEngine(rt, proptest.EngineOpts{})
		want.SampleCount = rapid.IntRange(0, 100000).Draw(rt, "sampleCount")
		want.SampleIntervalSeconds = rapid.SampledFrom([]int{0, 60, 300, 600, 3600, 86400}).
			Draw(rt, "sampleIntervalSeconds")

		// Timestamps are drawn as whole seconds. The binary codec stores time as
		// a Unix representation, so sub-second precision is not preserved and
		// generating it would fail the round trip for a reason that has nothing
		// to do with the fields under test.
		base := time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC)
		firstOffset := rapid.IntRange(0, 3600).Draw(rt, "firstOffsetSeconds")
		spanSeconds := rapid.IntRange(0, 604800).Draw(rt, "spanSeconds")
		want.FirstSampleTime = base.Add(time.Duration(firstOffset) * time.Second)
		want.LastSampleTime = want.FirstSampleTime.Add(time.Duration(spanSeconds) * time.Second)

		b, err := want.MarshalBinary()
		require.NoError(rt, err)

		got := new(kubemodel.InferenceEngine)
		require.NoError(rt, got.UnmarshalBinary(b))

		require.Equal(rt, want.PodUID, got.PodUID)
		require.Equal(rt, want.EngineIndex, got.EngineIndex)
		require.Equal(rt, want.NamespaceUID, got.NamespaceUID)
		require.Equal(rt, want.ModelName, got.ModelName)
		require.Equal(rt, want.Engine, got.Engine)

		// Bit-identical, not approximate: binary encoding is lossless, so any
		// drift here is a codec bug rather than float representation.
		require.Equal(rt, want.KVCacheUsageAvg, got.KVCacheUsageAvg)
		require.Equal(rt, want.KVCacheUsageP95, got.KVCacheUsageP95)
		require.Equal(rt, want.KVCacheUsageMax, got.KVCacheUsageMax)
		require.Equal(rt, want.QueueDepthAvg, got.QueueDepthAvg)
		require.Equal(rt, want.QueueDepthP95, got.QueueDepthP95)
		require.Equal(rt, want.QueueDepthMax, got.QueueDepthMax)
		require.Equal(rt, want.RunningRequestsAvg, got.RunningRequestsAvg)
		require.Equal(rt, want.RunningRequestsP95, got.RunningRequestsP95)
		require.Equal(rt, want.RunningRequestsMax, got.RunningRequestsMax)
		require.Equal(rt, want.Preemptions, got.Preemptions)

		// The version-4 additions.
		require.Equal(rt, want.SampleCount, got.SampleCount)
		require.Equal(rt, want.SampleIntervalSeconds, got.SampleIntervalSeconds)
		require.True(rt, want.FirstSampleTime.Equal(got.FirstSampleTime),
			"FirstSampleTime: want %s got %s", want.FirstSampleTime, got.FirstSampleTime)
		require.True(rt, want.LastSampleTime.Equal(got.LastSampleTime),
			"LastSampleTime: want %s got %s", want.LastSampleTime, got.LastSampleTime)
	})
}

// A version-3 payload predates the sample-window fields. A version-4 reader must
// decode it without error and leave those fields at their zero values.
//
// The fixture is a frozen historical artifact: it was captured from the
// version-3 encoder before the bump, and it can never be regenerated, because
// the current encoder emits version 4. See testdata/README.md.
func TestVersion3PayloadDecodesWithoutSampleWindowFields(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("testdata", "kubemodelset-v3.bin"))
	require.NoError(t, err, "the frozen version-3 fixture must be present; it cannot be regenerated")

	kms := new(kubemodel.KubeModelSet)
	require.NoError(t, kms.UnmarshalBinary(b),
		"a version-4 reader must decode a version-3 payload rather than erroring")

	// The fixture carries two engines on purpose. A single-engine fixture cannot
	// detect a mis-gated field: four stray bytes read for a field that should
	// have been skipped would land on trailing zeros, leaving the value at zero
	// and the assertion satisfied. With two engines, a stray read in the first
	// desynchronises the second, so the corruption becomes visible.
	//
	// This is not hypothetical. The first version of this test used a
	// single-engine fixture, and mutating the version gate from 4 to 3 did not
	// fail it.
	require.Len(t, kms.InferenceEngines, 2,
		"the fixture must carry two engines, or a mis-gated field cannot be detected")
	require.Equal(t, "v3-fixture-cluster", kms.Cluster.Name, "cluster decoded from the v3 payload")

	// Full expected state, so a desynchronised stream fails on a wrong value
	// rather than only on a non-zero sample count.
	want := map[string]struct {
		podUID, namespaceUID, modelName string
		kvAvg, kvP95, kvMax             float64
		queueAvg, queueP95, queueMax    float64
		runAvg, runP95, runMax          float64
		preemptions                     float64
	}{
		"pod-uid-alpha/0": {
			"pod-uid-alpha", "ns-uid-alpha", "org/model-alpha",
			0.11, 0.22, 0.33, 1.5, 2.5, 3, 11.5, 22.5, 32, 7,
		},
		"pod-uid-beta/1": {
			"pod-uid-beta", "ns-uid-beta", "org/model-beta",
			0.44, 0.55, 0.66, 4.5, 5.5, 6, 44.5, 55.5, 64, 13,
		},
	}

	for key, exp := range want {
		engine, ok := kms.InferenceEngines[key]
		require.True(t, ok, "engine %s missing from the decoded set", key)

		// Pre-version-4 fields must decode exactly. These are what catch a
		// desynchronised stream.
		require.Equal(t, exp.podUID, engine.PodUID, "engine %s", key)
		require.Equal(t, exp.namespaceUID, engine.NamespaceUID, "engine %s", key)
		require.Equal(t, exp.modelName, engine.ModelName, "engine %s", key)
		require.Equal(t, kubemodel.EngineVLLM, engine.Engine, "engine %s", key)
		require.Equal(t, exp.kvAvg, engine.KVCacheUsageAvg, "engine %s", key)
		require.Equal(t, exp.kvP95, engine.KVCacheUsageP95, "engine %s", key)
		require.Equal(t, exp.kvMax, engine.KVCacheUsageMax, "engine %s", key)
		require.Equal(t, exp.queueAvg, engine.QueueDepthAvg, "engine %s", key)
		require.Equal(t, exp.queueP95, engine.QueueDepthP95, "engine %s", key)
		require.Equal(t, exp.queueMax, engine.QueueDepthMax, "engine %s", key)
		require.Equal(t, exp.runAvg, engine.RunningRequestsAvg, "engine %s", key)
		require.Equal(t, exp.runP95, engine.RunningRequestsP95, "engine %s", key)
		require.Equal(t, exp.runMax, engine.RunningRequestsMax, "engine %s", key)
		require.Equal(t, exp.preemptions, engine.Preemptions, "engine %s", key)

		// The version-4 fields were not in the payload, so they must be zero
		// rather than filled with bytes belonging to the next field.
		require.Zero(t, engine.SampleCount, "engine %s: SampleCount must be zero in a v3 payload", key)
		require.Zero(t, engine.SampleIntervalSeconds,
			"engine %s: SampleIntervalSeconds must be zero in a v3 payload", key)
		require.True(t, engine.FirstSampleTime.IsZero(),
			"engine %s: FirstSampleTime must be zero in a v3 payload, got %s", key, engine.FirstSampleTime)
		require.True(t, engine.LastSampleTime.IsZero(),
			"engine %s: LastSampleTime must be zero in a v3 payload, got %s", key, engine.LastSampleTime)

		// HasSamples is the accessor callers should use instead of zero-testing a
		// gauge. On a v3 payload it must report false: the entry carries gauge
		// values but no evidence of how many samples produced them.
		require.False(t, engine.HasSamples(),
			"engine %s: a v3 payload carries no sample count, so HasSamples must be false", key)
	}
}

// Feature: gpu-inference-efficiency, Property 34: a payload above the
// implemented field version errors and returns no partial set — for any field
// version greater than the implemented version, decoding returns an error naming
// the versions rather than silently truncating.
func TestProperty34_PayloadAboveImplementedVersionErrors(t *testing.T) {
	start := time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC)
	kms := kubemodel.NewMockKubeModelSet(start, start.Add(time.Hour))

	b, err := kms.MarshalBinary()
	require.NoError(t, err)
	require.NotEmpty(t, b)

	offset := versionByteOffset(t, b)

	// Self-check before forging anything. If the offset calculation is wrong,
	// this fails rather than the test passing for the wrong reason — which is
	// exactly what happened on the first attempt at this test, where corrupting
	// byte 0 destroyed the string-table magic instead of the version and the
	// decoder read the corrupt magic byte as a version of 255. The test passed
	// while testing nothing about a well-formed newer payload.
	require.Equal(t, kubemodel.DefaultCodecVersion, b[offset],
		"located byte is not the codec version; the container layout this test assumes has changed")

	forged := make([]byte, len(b))
	copy(forged, b)
	forged[offset] = kubemodel.DefaultCodecVersion + 1

	act := new(kubemodel.KubeModelSet)
	err = act.UnmarshalBinary(forged)
	require.Error(t, err, "a payload above the implemented version must not decode silently")
	require.Contains(t, err.Error(), fmt.Sprintf("%d", kubemodel.DefaultCodecVersion+1),
		"the error must name the payload version it rejected")
	require.Contains(t, err.Error(), fmt.Sprintf("%d", kubemodel.DefaultCodecVersion),
		"the error must name the version this build implements")

	// No partial set: the decode target must not be left holding fragments of a
	// payload it refused. A caller that ignores the error should not find
	// half-populated data that looks usable.
	require.Empty(t, act.InferenceEngines, "a rejected payload must leave no partially decoded entries")
	require.Empty(t, act.Pods, "a rejected payload must leave no partially decoded entries")
	require.Nil(t, act.Cluster, "a rejected payload must leave no partially decoded entries")
}

// versionByteOffset returns the index of the codec version byte in an encoded
// payload.
//
// bingen prefixes the encoded struct with a string table, so the version is not
// at byte 0. The container is:
//
//	"BGST"                     4 bytes, the string-table tag
//	count                      int32, little-endian, number of strings
//	count x (len, bytes)       each string prefixed by a uint16 little-endian length
//	version                    uint8   <- what this returns
//
// This duplicates knowledge of the container layout, which is a real cost. It is
// paid because the alternative is a version-compatibility test that cannot
// actually construct a newer payload, and the caller's self-check on the located
// byte turns a layout change into a clear failure rather than a silent one.
func versionByteOffset(t *testing.T, b []byte) int {
	t.Helper()

	const tag = "BGST"
	require.GreaterOrEqual(t, len(b), len(tag)+4)
	require.Equal(t, tag, string(b[:len(tag)]), "payload does not begin with the bingen string-table tag")

	pos := len(tag)
	count := int(binary.LittleEndian.Uint32(b[pos : pos+4]))
	pos += 4

	for i := 0; i < count; i++ {
		require.LessOrEqual(t, pos+2, len(b), "string table truncated at entry %d", i)
		strLen := int(binary.LittleEndian.Uint16(b[pos : pos+2]))
		pos += 2 + strLen
	}

	require.Less(t, pos, len(b), "no version byte after the string table")
	return pos
}
