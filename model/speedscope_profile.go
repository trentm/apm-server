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
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/elastic/apm-server/datastreams"
	"github.com/elastic/apm-server/decoder"
	"github.com/elastic/apm-server/transform"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/gofrs/uuid"
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
	// XXX Timestamp troubles:
	// - The `startValue` is *not* spec'd to be the time the profile was taken
	//   in absolute value. That leaves getting the @timestamp elsewhere.
	//   We have "now" (i.e. roughly the intake time).
	// - As well, for the "duration" of the profile, there is no such field
	//   in the speedscope spec. If "unit" is a time, then possible "endValue"
	//   can be added to our start time, but that isn't always possible.
	// - Idea: could add separate intake API field for the profile start time?
	intakeTime := time.Now()

	frames := sp.File.Shared.Frames

	for _, profile := range sp.File.Profiles {

		// Generate a unique profile ID shared by all samples in the
		// profile.  If we can't generate a UUID for whatever reason,
		// omit the profile ID.
		var profileID string
		if uuid, err := uuid.NewV4(); err == nil {
			profileID = fmt.Sprintf("%x", uuid)
		}

		// XXX Duration: profile.go is doing `profileFields["duration"] = pp.Profile.DurationNanos`
		//     but fields.yml says `Duration of the profile, in microseconds.` What unit is correct?
		// XXX See "Timestamp troubles" above.
		//  duration := int64(profile.EndValue - profile.StartValue) ?
		duration := int64(42)

		var at float64
		frameIdStack := make(int64Stack, 0) // current stack of indeces into `frames`

		takeSample := func(startVal, endVal float64) {
			// Generic fields for all events.
			fields := mapStr{"processor": profileProcessorEntry}
			if cfg.DataStreams {
				fields[datastreams.TypeField] = datastreams.MetricsType
				dataset := fmt.Sprintf("%s.%s", ProfilesDataset, datastreams.NormalizeServiceName(sp.Metadata.Service.Name))
				fields[datastreams.DatasetField] = dataset
			}
			sp.Metadata.set(&fields, nil)

			// "profile" field
			profileFields := common.MapStr{}
			if profileID != "" {
				profileFields["id"] = profileID
			}
			profileFields["duration"] = duration
			// XXX Currently assuming this profile is measuring wall time, so set "wall.us".
			// XXX Is there a way we could tell if this is "cpu.ns"? Perhaps if unit is "nanoseconds"
			// XXX we can assume not "wall time"??  If "bytes" use alloc_space.bytes.
			// XXX If "none" do we use "alloc_objects.count" or "inuse_objects.count" or ...?
			profileFields["wall.us"] = int64(endVal - startVal) // XXX adjust for units given
			profileFields["samples.count"] = int64(1)
			if frameIdStack.Len() > 0 {
				hash := xxhash.New()
				stack := make([]common.MapStr, frameIdStack.Len())
				for i, frameId := range frameIdStack {
					frame := frames[frameId]
					log.Printf("XXX add stack entry for frameId=%d frame=%#v\n", frameId, frames[frameId])
					hash.WriteString(frame.Name)
					frameFields := mapStr{
						"id":       fmt.Sprintf("%x", hash.Sum(nil)),
						"function": frame.Name,
					}
					if frameFields.maybeSetString("filename", frame.File) {
						if frame.Line > 0 {
							fields.set("line", frame.Line)
						}
					}
					stack[i] = common.MapStr(frameFields)

				}
				profileFields["stack"] = stack
				profileFields["top"] = stack[0]
			}
			fields[profileDocType] = profileFields

			events = append(events, beat.Event{
				Timestamp: intakeTime,
				Fields:    common.MapStr(fields),
			})
		}

		for i, evt := range profile.Events {
			if i == 0 {
				// XXX assert evt.Type == "O"
				at = evt.At
				frameIdStack.Push(evt.Frame)
				continue
			} else if evt.At != at {
				// We are "at" a new timestamp, the current
				// "frameIdStack" is a sample.
				takeSample(at, evt.At)
			}

			at = evt.At
			if evt.Type == "O" {
				frameIdStack.Push(evt.Frame)
			} else {
				if frameIdStack.Len() == 0 {
					// XXX log error here; how to get apm-server's logger?
					goto AbortEarly
				}
				popped := frameIdStack.Pop()
				if popped != evt.Frame {
					// XXX log error here
					goto AbortEarly
				}
			}
		}
	}

AbortEarly:
	return events
}

type int64Stack []int64

func (s *int64Stack) Push(v int64) {
	*s = append(*s, v)
}

func (s *int64Stack) Pop() int64 {
	l := len(*s)
	if l == 0 {
		panic("empty stack")
	}
	v := (*s)[l-1]
	*s = (*s)[:l-1]
	return v
}

func (s *int64Stack) Peek() int64 {
	l := len(*s)
	if l == 0 {
		panic("empty stack")
	}
	return (*s)[l-1]
}

func (s *int64Stack) Len() int {
	return len(*s)
}
