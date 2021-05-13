// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package model_test

import (
	"bytes"
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/apm-server/model"
	"github.com/elastic/apm-server/transform"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
)

// This is https://github.com/jlfwong/speedscope/blob/main/sample/profiles/speedscope/0.0.1/simple.speedscope.json
// with some whitespace edits for brevity.
var simpleEvented = []byte(`{
    "$schema": "https://www.speedscope.app/file-format-schema.json",
    "profiles": [
        {
            "endValue": 14,
            "events": [
                { "at": 0, "frame": 0, "type": "O" },
                { "at": 0, "frame": 1, "type": "O" },
                { "at": 0, "frame": 2, "type": "O" },
                { "at": 2, "frame": 2, "type": "C" },
                { "at": 2, "frame": 3, "type": "O" },
                { "at": 6, "frame": 3, "type": "C" },
                { "at": 6, "frame": 2, "type": "O" },
                { "at": 9, "frame": 2, "type": "C" },
                { "at": 14, "frame": 1, "type": "C" },
                { "at": 14, "frame": 0, "type": "C" }
            ],
            "name": "simple.txt",
            "startValue": 0,
            "type": "evented",
            "unit": "none"
        }
    ],
    "shared": {
        "frames": [
            { "name": "a" },
            { "name": "b" },
            { "name": "c" },
            { "name": "d" }
        ]
    },
    "version": "0.0.1"
}`)

var simpleSampled = []byte(`{
  "exporter": "speedscope@0.6.0",
  "$schema": "https://www.speedscope.app/file-format-schema.json",
  "name": "Two Samples",
  "activeProfileIndex": 1,
  "profiles": [
    {
      "type": "sampled",
      "name": "one",
      "unit": "seconds",
      "startValue": 0,
      "endValue": 14,
      "samples": [[0, 1, 2], [0, 1, 2], [0, 1, 3], [0, 1, 2], [0, 1]],
      "weights": [1, 1, 4, 3, 5]
    },
    {
      "type": "sampled",
      "name": "two",
      "unit": "seconds",
      "startValue": 0,
      "endValue": 14,
      "samples": [[0, 1, 2], [0, 1, 2], [0, 1, 3], [0, 1, 2], [0, 1]],
      "weights": [1, 1, 4, 3, 5]
    }
  ],
  "shared": {
    "frames": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]
  }
}`)

func TestParseSpeedscopeFileWithEvented(t *testing.T) {
	r := bytes.NewReader(simpleEvented)
	sf, err := model.ParseSpeedscopeFile(r)
	if assert.NoError(t, err) {
		assert.Len(t, sf.Profiles, 1)
		p, ok := sf.Profiles[0].(*model.EventedProfile)
		assert.True(t, ok)
		assert.Equal(t, model.Evented, p.Type)
		assert.Len(t, sf.Shared.Frames, 4)
	}
}

func TestParseSpeedscopeFileWithSampled(t *testing.T) {
	r := bytes.NewReader(simpleSampled)
	sf, err := model.ParseSpeedscopeFile(r)
	if assert.NoError(t, err) {
		assert.Len(t, sf.Profiles, 2)
		p, ok := sf.Profiles[0].(*model.SampledProfile)
		assert.True(t, ok)
		assert.Equal(t, model.Sampled, p.Type)
		assert.Len(t, p.Samples, 5)
		assert.Len(t, p.Samples[0], 3)
		assert.Len(t, p.Weights, 5)
		assert.Len(t, sf.Shared.Frames, 4)
	}
}

func TestSpeedscopeProfileTransform(t *testing.T) {
	r := bytes.NewReader(simpleEvented)
	sf, err := model.ParseSpeedscopeFile(r)
	if err != nil {
		t.Errorf("ParseSpeedscopeFile failed: %s", err)
		return
	}
	serviceName, env := "myService", "staging"
	service := model.Service{
		Name:        serviceName,
		Environment: env,
	}
	sp := model.SpeedscopeProfileEvent{
		Metadata: model.Metadata{Service: service},
		File:     sf,
	}

	batch := &model.Batch{SpeedscopeProfiles: []*model.SpeedscopeProfileEvent{&sp}}
	output := batch.Transform(context.Background(), &transform.Config{DataStreams: true})
	for i, e := range output {
		log.Printf("XXX output[%d].Fields: %v\n", i, e.Fields)
	}

	require.Len(t, output, 4)

	if profileMap, ok := output[0].Fields["profile"].(common.MapStr); ok {
		assert.NotZero(t, profileMap["id"])
		profileMap["id"] = "random"
	}

	assert.Equal(t, beat.Event{
		Timestamp: output[0].Timestamp,
		Fields: common.MapStr{
			"data_stream.type":    "metrics",
			"data_stream.dataset": "apm.profiling.myservice",
			"processor":           common.MapStr{"event": "profile", "name": "profile"},
			"service": common.MapStr{
				"name":        "myService",
				"environment": "staging",
			},
			"profile": common.MapStr{
				"id":            "random",
				"duration":      float64(14),
				"wall.us":       int64(2),
				"samples.count": int64(1),
				"top": common.MapStr{
					"function": "a",
					"id":       "d24ec4f1a98c6e5b",
				},
				"stack": []common.MapStr{{
					"function": "a",
					"id":       "d24ec4f1a98c6e5b",
				}, {
					"function": "b",
					"id":       "65f708ca92d04a61",
				}, {
					"function": "c",
					"id":       "44bc2cf5ad770999",
				}},
			},
		},
	}, output[0])
}
