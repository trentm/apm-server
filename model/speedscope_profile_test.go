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

	"github.com/elastic/apm-server/model"
	"github.com/elastic/apm-server/transform"
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

func TestParseSpeedscopeFile(t *testing.T) {
	r := bytes.NewReader(simpleEvented)
	sf, err := model.ParseSpeedscopeFile(r)
	if assert.NoError(t, err) {
		assert.Len(t, sf.Profiles, 1)
		assert.Equal(t, sf.Profiles[0].Type, "evented")
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
	log.Printf("XXX output: %#v\n", output)

	// XXX uncomment this when done first pass of `appendBeatEvents`
	// require.Len(t, output, 5)

	// if profileMap, ok := output[0].Fields["profile"].(common.MapStr); ok {
	// 	assert.NotZero(t, profileMap["id"])
	// 	profileMap["id"] = "random"
	// }

	// // XXX adjust the expected values here (currently cut 'n pasted from profile_test.go)
	// assert.Equal(t, beat.Event{
	// 	Timestamp: timestamp,
	// 	Fields: common.MapStr{
	// 		"data_stream.type":    "metrics",
	// 		"data_stream.dataset": "apm.profiling.myservice",
	// 		"processor":           common.MapStr{"event": "profile", "name": "profile"},
	// 		"service": common.MapStr{
	// 			"name":        "myService",
	// 			"environment": "staging",
	// 		},
	// 		"labels": common.MapStr{
	// 			"key1": []string{"abc", "def"},
	// 			"key2": []string{"ghi"},
	// 		},
	// 		"profile": common.MapStr{
	// 			"id":                "random",
	// 			"duration":          int64(10 * time.Second),
	// 			"cpu.ns":            int64(123),
	// 			"wall.us":           int64(789),
	// 			"inuse_space.bytes": int64(456),
	// 			"samples.count":     int64(1),
	// 			"top": common.MapStr{
	// 				"function": "foo",
	// 				"filename": "foo.go",
	// 				"line":     int64(1),
	// 				"id":       "98430081820ed765",
	// 			},
	// 			"stack": []common.MapStr{{
	// 				"function": "foo",
	// 				"filename": "foo.go",
	// 				"line":     int64(1),
	// 				"id":       "98430081820ed765",
	// 			}, {
	// 				"function": "bar",
	// 				"filename": "bar.go",
	// 				"id":       "48a37c90ad27a659",
	// 			}},
	// 		},
	// 	},
	// }, output[0])
}
