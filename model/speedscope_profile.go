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

package model

import (
	"fmt"
	"io"
	"log"

	"github.com/elastic/apm-server/decoder"
	"github.com/elastic/apm-server/transform"
	"github.com/elastic/beats/v7/libbeat/beat"
)

// SpeedscopeProfileEvent represents a speedscope profile file uploaded to the intake API.
type SpeedscopeProfileEvent struct {
	Metadata Metadata
	File     *SpeedscopeFile
}

// XXX This parsing and raw speedscope struct could move to a separate internal package. That move would help with SpeedscopeProfile name collison.
// https://www.speedscope.app/file-format-schema.json
// https://github.com/jlfwong/speedscope/blob/main/src/lib/file-format-spec.ts

const speedscopeSchema = "https://www.speedscope.app/file-format-schema.json"

// XXX Covers both OpenFrameEvent and CloseFrameEvent.
type SpeedscopeFrameEvent struct {
	Type  string  `json:"type"`  // XXX enum 'O' or 'C'; could we use "byte" here?
	At    float64 `json:"at"`    // XXX can this be int64?
	Frame int64   `json:"frame"` // Index into frames array.
}

// XXX How to handle switch on EventedProfile | SampledProfile? Currently just handling EventedProfile
type SpeedscopeProfile struct {
	Type       string                 `json:"type"` // XXX enum 'evented' or 'sampled'
	Name       string                 `json:"name"`
	Unit       string                 `json:"unit"`       // XXX enum
	StartValue float64                `json:"startValue"` // XXX not sure if can be int64
	EndValue   float64                `json:"endValue"`   // XXX not sure if can be int64
	Events     []SpeedscopeFrameEvent `json:"events"`
}
type SpeedscopeFrame struct {
	Name string `json:"name"`
	File string `json:"file"`
	Line int64  `json:"line"`
	Col  int64  `json:"col"`
}
type SpeedscopeShared struct {
	Frames []SpeedscopeFrame `json:"frames"`
}
type SpeedscopeFile struct {
	Schema   string              `json:"$schema"`
	Shared   SpeedscopeShared    `json:"shared"`
	Profiles []SpeedscopeProfile `json:"profiles"`
}

func ParseSpeedscopeFile(r io.Reader) (*SpeedscopeFile, error) {
	var sf SpeedscopeFile

	dec := decoder.NewJSONDecoder(r)
	if err := dec.Decode(&sf); err != nil {
		return nil, err
	}

	// Validations
	if sf.Schema != speedscopeSchema {
		return nil, fmt.Errorf("invalid speedscope schema: '%s'", sf.Schema)
	}
	for _, profile := range sf.Profiles {
		if profile.Type != "evented" {
			// Support for 'sampled' profile type is not yet implemented.
			return nil, fmt.Errorf("unsupported speedscope profile type: '%s'", profile.Type)
		}
		// XXX validate/normalize profile.Unit
	}
	return &sf, nil
}

// appendBeatEvents transforms a SpeedscopeFile into a sequence of beat.Events
// (one per profile sampled stack), and appends them to events.
func (sp SpeedscopeProfileEvent) appendBeatEvents(cfg *transform.Config, events []beat.Event) []beat.Event {
	log.Printf("XXX NYI appendBeatEvents\n")

	// for _, profile := range sp.File.Profiles {

	// 	// XXX Spec says profile.startValue is "typically a timestamp"
	// 	//     and the (optional) event "at" values, presumably, are the same "unit".
	// 	//     Do we need some sanity checks that these aren't non-timestamp values?

	// 	// Generate a unique profile ID shared by all samples in the
	// 	// profile.  If we can't generate a UUID for whatever reason,
	// 	// omit the profile ID.
	// 	var profileID string
	// 	if uuid, err := uuid.NewV4(); err == nil {
	// 		profileID = fmt.Sprintf("%x", uuid)
	// 	}

	// 	for _, evt := range profile.Events {
	// 		profileFields := common.MapStr{}
	// 		if profileID != "" {
	// 			profileFields["id"] = profileID
	// 		}

	// 		// XXX Duration: profile.go is doing `profileFields["duration"] = pp.Profile.DurationNanos`
	// 		//     but fields.yml says `Duration of the profile, in microseconds.` What unit is correct?
	// 		// TODO: calc from `(StartValue - EndValue) * scale from Unit`
	// 		// profileFields["duration"] = ...

	// 		// From this:
	// 		// XXX note: bug in speedscope export that loses the "line" value?
	// 		//
	// 		// "shared": {
	// 		// 	"frames": [
	// 		// 	  {
	// 		// 	    "name": "parserOnIncoming",
	// 		// 	    "file": "_http_server.js",
	// 		// 	    "line": 0
	// 		// 	  },
	// 		// 	{
	// 		// 		"name": "Writable.write",
	// 		// 		"file": "/Users/trentm/el/stream-chopper/node_modules/readable-stream/lib/_stream_writable.js",
	// 		// 		"line": 0
	// 		// 	},
	// 		// "events": [
	// 		// 	{
	// 		// 	  "type": "O",
	// 		// 	  "frame": 0,
	// 		// 	  "at": 0
	// 		// 	},
	// 		// 	{
	// 		// 	  "type": "C",
	// 		// 	  "frame": 0,
	// 		// 	  "at": 1000
	// 		// 	},
	// 		//
	// 		// To this:
	// 		//
	// 		// "profile": {
	// 		// 	"duration": 10208325000,
	// 		// 	"stack": [
	// 		// 	  {
	// 		// 	    "filename": "/Users/trentm/el/apm-nodejs-dev/perf/bin/pathosapp.js",
	// 		// 	    "line": 147,
	// 		// 	    "function": "err",
	// 		// 	    "id": "e0b9a208dc1a04e5"
	// 		// 	  },
	// 		// 	  ...
	// 		// 	],
	// 		// 	"top": {
	// 		// 		"filename": "/Users/trentm/el/apm-nodejs-dev/perf/bin/pathosapp.js",
	// 		// 		"line": 147,
	// 		// 		"function": "err",
	// 		// 		"id": "e0b9a208dc1a04e5"
	// 		// 	},
	// 		// 	"id": "664f8b705db34cdf824ae621dc841866",
	// 		// 	"wall.us": 1000,
	// 		// 	"samples.count": 1
	// 		// },
	// 	}
	// }

	return events
}
