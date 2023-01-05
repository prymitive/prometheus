// Copyright 2021 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

func TestBoundedChunk(t *testing.T) {
	tests := []struct {
		name           string
		inputChunk     chunkenc.Chunk
		inputMinT      int64
		inputMaxT      int64
		initialSeek    int64
		seekIsASuccess bool
		expSamples     []sample
	}{
		{
			name:       "if there are no samples it returns nothing",
			inputChunk: newTestChunk(0),
			expSamples: nil,
		},
		{
			name:       "bounds represent a single sample",
			inputChunk: newTestChunk(10),
			expSamples: []sample{
				{0, 0, nil, nil},
			},
		},
		{
			name:       "if there are bounds set only samples within them are returned",
			inputChunk: newTestChunk(10),
			inputMinT:  1,
			inputMaxT:  8,
			expSamples: []sample{
				{1, 1, nil, nil},
				{2, 2, nil, nil},
				{3, 3, nil, nil},
				{4, 4, nil, nil},
				{5, 5, nil, nil},
				{6, 6, nil, nil},
				{7, 7, nil, nil},
				{8, 8, nil, nil},
			},
		},
		{
			name:       "if bounds set and only maxt is less than actual maxt",
			inputChunk: newTestChunk(10),
			inputMinT:  0,
			inputMaxT:  5,
			expSamples: []sample{
				{0, 0, nil, nil},
				{1, 1, nil, nil},
				{2, 2, nil, nil},
				{3, 3, nil, nil},
				{4, 4, nil, nil},
				{5, 5, nil, nil},
			},
		},
		{
			name:       "if bounds set and only mint is more than actual mint",
			inputChunk: newTestChunk(10),
			inputMinT:  5,
			inputMaxT:  9,
			expSamples: []sample{
				{5, 5, nil, nil},
				{6, 6, nil, nil},
				{7, 7, nil, nil},
				{8, 8, nil, nil},
				{9, 9, nil, nil},
			},
		},
		{
			name:           "if there are bounds set with seek before mint",
			inputChunk:     newTestChunk(10),
			inputMinT:      3,
			inputMaxT:      7,
			initialSeek:    1,
			seekIsASuccess: true,
			expSamples: []sample{
				{3, 3, nil, nil},
				{4, 4, nil, nil},
				{5, 5, nil, nil},
				{6, 6, nil, nil},
				{7, 7, nil, nil},
			},
		},
		{
			name:           "if there are bounds set with seek between mint and maxt",
			inputChunk:     newTestChunk(10),
			inputMinT:      3,
			inputMaxT:      7,
			initialSeek:    5,
			seekIsASuccess: true,
			expSamples: []sample{
				{5, 5, nil, nil},
				{6, 6, nil, nil},
				{7, 7, nil, nil},
			},
		},
		{
			name:           "if there are bounds set with seek after maxt",
			inputChunk:     newTestChunk(10),
			inputMinT:      3,
			inputMaxT:      7,
			initialSeek:    8,
			seekIsASuccess: false,
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("name=%s", tc.name), func(t *testing.T) {
			chunk := boundedChunk{tc.inputChunk, tc.inputMinT, tc.inputMaxT}

			// Testing Bytes()
			expChunk := chunkenc.NewXORChunk()
			if tc.inputChunk.NumSamples() > 0 {
				app, err := expChunk.Appender()
				require.NoError(t, err)
				for ts := tc.inputMinT; ts <= tc.inputMaxT; ts++ {
					app.Append(ts, float64(ts))
				}
			}
			require.Equal(t, expChunk.Bytes(), chunk.Bytes())

			var samples []sample
			it := chunk.Iterator(nil)

			if tc.initialSeek != 0 {
				// Testing Seek()
				val := it.Seek(tc.initialSeek)
				require.Equal(t, tc.seekIsASuccess, val == chunkenc.ValFloat)
				if val == chunkenc.ValFloat {
					t, v := it.At()
					samples = append(samples, sample{t, v, nil, nil})
				}
			}

			// Testing Next()
			for it.Next() == chunkenc.ValFloat {
				t, v := it.At()
				samples = append(samples, sample{t, v, nil, nil})
			}

			// it.Next() should keep returning no  value.
			for i := 0; i < 10; i++ {
				require.True(t, it.Next() == chunkenc.ValNone)
			}

			require.Equal(t, tc.expSamples, samples)
		})
	}
}

func newTestChunk(numSamples int) chunkenc.Chunk {
	xor := chunkenc.NewXORChunk()
	a, _ := xor.Appender()
	for i := 0; i < numSamples; i++ {
		a.Append(int64(i), float64(i))
	}
	return xor
}

// TestMemSeries_chunk runs a series of tests on memSeries.chunk() calls.
// It will simulate various conditions to ensure all code paths in that function are covered.
func TestMemSeries_chunk(t *testing.T) {
	type setupFn func(*memSeries, *chunks.ChunkDiskMapper)

	type callOutput uint8
	const (
		outHeadChunk    callOutput = iota // memSeries.chunk() call returned memSeries.headChunk with garbageCollect=false
		outMmappedChunk                   // memSeries.chunk() call returned a chunk from memSeries.mmappedChunks with garbageCollect=true
		outErr                            // memSeries.chunk() call returned an error
	)

	tests := []struct {
		name          string
		setup         setupFn            // optional function called just before the test memSeries.chunk() call
		mmappedChunks int                // the number of mmapped chunks to create on memSeries by appending enough samples
		inputID       chunks.HeadChunkID // requested chunk id for memSeries.chunk() call
		expected      callOutput
	}{
		{
			name: "call ix=0 on empty memSeries",
			setup: func(s *memSeries, cdm *chunks.ChunkDiskMapper) {
				s.headChunk = nil
			},
			inputID:  0,
			expected: outErr,
		},
		{
			name: "call ix=1 on empty memSeries",
			setup: func(s *memSeries, cdm *chunks.ChunkDiskMapper) {
				s.headChunk = nil
			},
			inputID:  1,
			expected: outErr,
		},
		{
			name: "firstChunkID > ix",
			setup: func(s *memSeries, cdm *chunks.ChunkDiskMapper) {
				s.firstChunkID = 5
			},
			inputID:  1,
			expected: outErr,
		},
		{
			name:     "call ix=0 on memSeries with no mmapped chunks",
			inputID:  0,
			expected: outHeadChunk,
		},
		{
			name:     "call ix=1 on memSeries with no mmapped chunks",
			inputID:  1,
			expected: outErr,
		},
		{
			name:     "call ix=10 on memSeries with no mmapped chunks",
			inputID:  10,
			expected: outErr,
		},
		{
			name:          "call ix=0 on memSeries with 3 mmapped chunks",
			mmappedChunks: 3,
			inputID:       0,
			expected:      outMmappedChunk,
		},
		{
			name:          "call ix=1 on memSeries with 3 mmapped chunks",
			mmappedChunks: 3,
			inputID:       1,
			expected:      outMmappedChunk,
		},
		{
			name:          "call ix=3 on memSeries with 3 mmapped chunks",
			mmappedChunks: 3,
			inputID:       3,
			expected:      outHeadChunk,
		},
		{
			name:          "call ix=0 on memSeries with 3 mmapped chunks and no headChunk",
			mmappedChunks: 3,
			setup: func(s *memSeries, cdm *chunks.ChunkDiskMapper) {
				s.headChunk = nil
			},
			inputID:  0,
			expected: outMmappedChunk,
		},
		{
			name:          "call ix=2 on memSeries with 3 mmapped chunks and no headChunk",
			mmappedChunks: 3,
			setup: func(s *memSeries, cdm *chunks.ChunkDiskMapper) {
				s.headChunk = nil
			},
			inputID:  2,
			expected: outMmappedChunk,
		},
		{
			name:          "call ix=3 on memSeries with 3 mmapped chunks and no headChunk",
			mmappedChunks: 3,
			setup: func(s *memSeries, cdm *chunks.ChunkDiskMapper) {
				s.headChunk = nil
			},
			inputID:  3,
			expected: outErr,
		},
		{
			name:          "call ix=1 on memSeries with 3 mmapped chunks and closed ChunkDiskMapper",
			mmappedChunks: 3,
			setup: func(s *memSeries, cdm *chunks.ChunkDiskMapper) {
				cdm.Close()
			},
			inputID:  1,
			expected: outErr,
		},
		{
			name:          "call ix=3 on memSeries with 3 mmapped chunks and closed ChunkDiskMapper",
			mmappedChunks: 3,
			setup: func(s *memSeries, cdm *chunks.ChunkDiskMapper) {
				cdm.Close()
			},
			inputID:  3,
			expected: outHeadChunk,
		},
	}

	memChunkPool := &sync.Pool{
		New: func() interface{} {
			return &memChunk{}
		},
	}

	const chunkRange = 100
	const chunkStep = 5

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			chunkDiskMapper, err := chunks.NewChunkDiskMapper(nil, dir, chunkenc.NewPool(), chunks.DefaultWriteBufferSize, chunks.DefaultWriteQueueSize)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, chunkDiskMapper.Close())
			}()

			series := newMemSeries(labels.EmptyLabels(), 1, true)

			// Write enough samples to create headChunk and the requested number of mmapped chunks.
			// Running this:
			//   i := 0; i < tc.mmappedChunks*chunkRange
			// would give us exactly the number of samples needed to fill all mmapped chunks.
			// We have one extra iteration (i<= instead of i<) to write an extra sample that will trigger
			// mmapping of last chunk and creation of new headChunk where it will be appended.
			for i := 0; i <= tc.mmappedChunks*chunkRange; i += chunkStep {
				ok, _ := series.append(int64(i), float64(i), 0, chunkDiskMapper, chunkRange)
				require.True(t, ok, "sample append failed")
			}
			require.NotNil(t, series.headChunk, "head chunk is missing")
			require.Equal(t, tc.mmappedChunks, len(series.mmappedChunks), "wrong number of mmapped chunks")

			if tc.setup != nil {
				tc.setup(series, chunkDiskMapper)
			}

			chk, gc, err := series.chunk(tc.inputID, chunkDiskMapper, memChunkPool)
			switch tc.expected {
			case outHeadChunk:
				require.NoError(t, err, "unexpected error")
				require.False(t, gc, "expected a chunk with gc=false but got gc=%v", gc)
				// there's only a single sample appended to headChunk
				require.Equal(t, int64(tc.inputID*chunkRange), chk.minTime, "wrong chunk minTime returned")
				require.Equal(t, int64(tc.inputID*chunkRange), chk.maxTime, "wrong chunk maxTime returned")
			case outMmappedChunk:
				require.NoError(t, err, "unexpected error")
				require.True(t, gc, "expected a chunk with gc=true but got gc=%v", gc)
				// all mmapped chunks should be full and cover ..0 .95 range
				require.Equal(t, int64(tc.inputID*chunkRange), chk.minTime, "wrong chunk minTime returned")
				require.Equal(t, int64((tc.inputID+1)*chunkRange-chunkStep), chk.maxTime, "wrong chunk maxTime returned")
			case outErr:
				require.Nil(t, chk, "got a non-nil chunk reference returned with an error")
				require.Error(t, err)
			}
		})
	}
}
