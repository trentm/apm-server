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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"reflect"
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

type SpeedscopeFile struct {
	Schema             *string  `json:"$schema"`
	ActiveProfileIndex *float64 `json:"activeProfileIndex,omitempty"`
	Exporter           *string  `json:"exporter,omitempty"`
	Name               *string  `json:"name,omitempty"`
	// collection of EventedProfile or SampledProfile
	Profiles []Profile `json:"profiles"`
	Shared   Shared    `json:"shared"`
}

type Profile interface {
	isProfile()
}

type EventedProfile struct {
	EndValue   float64      `json:"endValue"`
	Events     []FrameEvent `json:"events"`
	Name       string       `json:"name"`
	StartValue float64      `json:"startValue"`
	Type       ProfileType  `json:"type"`
	Unit       ValueUnit    `json:"unit"`
}

func (e EventedProfile) isProfile() {}

type SampledProfile struct {
	EndValue   float64     `json:"endValue"`
	Name       string      `json:"name"`
	Samples    [][]float64 `json:"samples"`
	StartValue float64     `json:"startValue"`
	Type       ProfileType `json:"type"`
	Unit       ValueUnit   `json:"unit"`
	Weights    []float64   `json:"weights"`
}

func (e SampledProfile) isProfile() {}

type FrameEvent struct {
	At    float64        `json:"at"`
	Frame int64          `json:"frame"`
	Type  FrameEventType `json:"type"`
}

type Shared struct {
	Frames []Frame `json:"frames"`
}

type Frame struct {
	Col  *int64  `json:"col,omitempty"`
	File *string `json:"file,omitempty"`
	Line *int64  `json:"line,omitempty"`
	Name string  `json:"name"`
}

type FrameEventType string

const Close FrameEventType = "C"
const Open FrameEventType = "O"

var eventTypes = []interface{}{
	"C",
	"O",
}

type ProfileType string

const Evented ProfileType = "evented"
const Sampled ProfileType = "sampled"

var profileTypes = []interface{}{
	"evented",
	"sampled",
}

type ValueUnit string

const None ValueUnit = "none"
const Bytes ValueUnit = "bytes"
const Microseconds ValueUnit = "microseconds"
const Milliseconds ValueUnit = "milliseconds"
const Nanoseconds ValueUnit = "nanoseconds"
const Seconds ValueUnit = "seconds"

var valueUnits = []interface{}{
	"bytes",
	"microseconds",
	"milliseconds",
	"nanoseconds",
	"none",
	"seconds",
}

func (j *ValueUnit) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	var ok bool
	for _, expected := range valueUnits {
		if reflect.DeepEqual(v, expected) {
			ok = true
			break
		}
	}
	if !ok {
		return fmt.Errorf("invalid value (expected one of %#v): %#v", valueUnits, v)
	}
	*j = ValueUnit(v)
	return nil
}

func (p *SpeedscopeFile) UnmarshalJSON(b []byte) error {
	var objMap map[string]*json.RawMessage
	err := json.Unmarshal(b, &objMap)
	if err != nil {
		return err
	}

	val, ok := objMap["$schema"]
	if !ok {
		return fmt.Errorf("field $schema is required")
	}
	var schema string
	err = json.Unmarshal(*val, &schema)
	if err != nil {
		return err
	}
	p.Schema = &schema

	val, ok = objMap["shared"]
	if !ok {
		return fmt.Errorf("field shared is required")
	}
	var shared Shared
	err = json.Unmarshal(*val, &shared)
	if err != nil {
		return err
	}
	p.Shared = shared

	// handle profiles union type
	val, ok = objMap["profiles"]
	if !ok {
		return fmt.Errorf("field profiles is required")
	}
	var profiles []*json.RawMessage
	err = json.Unmarshal(*val, &profiles)
	if err != nil {
		return err
	}

	p.Profiles = make([]Profile, len(profiles))
	var raw map[string]interface{}
	for index, rawMessage := range profiles {
		err = json.Unmarshal(*rawMessage, &raw)
		if err != nil {
			return err
		}
		if raw["type"] == "evented" {
			var e EventedProfile
			err := json.Unmarshal(*rawMessage, &e)
			if err != nil {
				return err
			}
			p.Profiles[index] = &e
		} else if raw["type"] == "sampled" {
			var s SampledProfile
			err := json.Unmarshal(*rawMessage, &s)
			if err != nil {
				return err
			}
			p.Profiles[index] = &s
		} else {
			return fmt.Errorf("unsupported type %#v", raw["type"])
		}
	}

	if val, ok := objMap["name"]; ok {
		var name string
		err = json.Unmarshal(*val, &name)
		if err != nil {
			return err
		}
		p.Name = &name
	}

	if val, ok := objMap["exporter"]; ok {
		var exporter string
		err = json.Unmarshal(*val, &exporter)
		if err != nil {
			return err
		}
		p.Exporter = &exporter
	}

	if val, ok := objMap["activeProfileIndex"]; ok {
		var activeProfileIndex float64
		err = json.Unmarshal(*val, &activeProfileIndex)
		if err != nil {
			return err
		}
		p.ActiveProfileIndex = &activeProfileIndex
	}

	return nil
}

func (e *SampledProfile) UnmarshalJSON(b []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if v, ok := raw["endValue"]; !ok || v == nil {
		return fmt.Errorf("field endValue: required")
	}
	if v, ok := raw["name"]; !ok || v == nil {
		return fmt.Errorf("field name: required")
	}
	if v, ok := raw["samples"]; !ok || v == nil {
		return fmt.Errorf("field samples: required")
	}
	if v, ok := raw["startValue"]; !ok || v == nil {
		return fmt.Errorf("field startValue: required")
	}
	if v, ok := raw["type"]; !ok || v == nil {
		return fmt.Errorf("field type: required")
	}
	if v, ok := raw["unit"]; !ok || v == nil {
		return fmt.Errorf("field unit: required")
	}
	if v, ok := raw["weights"]; !ok || v == nil {
		return fmt.Errorf("field weights: required")
	}
	type Plain SampledProfile
	var plain Plain
	if err := json.Unmarshal(b, &plain); err != nil {
		return err
	}
	*e = SampledProfile(plain)
	return nil
}

func (e *EventedProfile) UnmarshalJSON(b []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if v, ok := raw["endValue"]; !ok || v == nil {
		return fmt.Errorf("field endValue: required")
	}
	if v, ok := raw["events"]; !ok || v == nil {
		return fmt.Errorf("field events: required")
	}
	if v, ok := raw["name"]; !ok || v == nil {
		return fmt.Errorf("field name: required")
	}
	if v, ok := raw["startValue"]; !ok || v == nil {
		return fmt.Errorf("field startValue: required")
	}
	if v, ok := raw["type"]; !ok || v == nil {
		return fmt.Errorf("field type: required")
	}
	if v, ok := raw["unit"]; !ok || v == nil {
		return fmt.Errorf("field unit: required")
	}
	type Plain EventedProfile
	var plain Plain
	if err := json.Unmarshal(b, &plain); err != nil {
		return err
	}
	*e = EventedProfile(plain)
	return nil
}

func (j *FrameEventType) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	var ok bool
	for _, expected := range eventTypes {
		if reflect.DeepEqual(v, expected) {
			ok = true
			break
		}
	}
	if !ok {
		return fmt.Errorf("invalid value (expected one of %#v): %#v", eventTypes, v)
	}
	*j = FrameEventType(v)
	return nil
}

func (j *Frame) UnmarshalJSON(b []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if v, ok := raw["name"]; !ok || v == nil {
		return fmt.Errorf("field name: required")
	}
	type Plain Frame
	var plain Plain
	if err := json.Unmarshal(b, &plain); err != nil {
		return err
	}
	*j = Frame(plain)
	return nil
}

func (j *FrameEvent) UnmarshalJSON(b []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if v, ok := raw["at"]; !ok || v == nil {
		return fmt.Errorf("field at: required")
	}
	if v, ok := raw["frame"]; !ok || v == nil {
		return fmt.Errorf("field frame: required")
	}
	if v, ok := raw["type"]; !ok || v == nil {
		return fmt.Errorf("field type: required")
	}
	type Plain FrameEvent
	var plain Plain
	if err := json.Unmarshal(b, &plain); err != nil {
		return err
	}
	*j = FrameEvent(plain)
	return nil
}

func (j *Shared) UnmarshalJSON(b []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if v, ok := raw["frames"]; !ok || v == nil {
		return fmt.Errorf("field frames: required")
	}
	type Plain Shared
	var plain Plain
	if err := json.Unmarshal(b, &plain); err != nil {
		return err
	}
	*j = Shared(plain)
	return nil
}

func (j *ProfileType) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	var ok bool
	for _, expected := range profileTypes {
		if reflect.DeepEqual(v, expected) {
			ok = true
			break
		}
	}
	if !ok {
		return fmt.Errorf("invalid value (expected one of %#v): %#v", profileTypes, v)
	}
	*j = ProfileType(v)
	return nil
}

func ParseSpeedscopeFile(r io.Reader) (*SpeedscopeFile, error) {
	var sf SpeedscopeFile

	dec := decoder.NewJSONDecoder(r)
	if err := dec.Decode(&sf); err != nil {
		return nil, err
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

		// XXX only handle eventedProfiles for now
		if eventedProfile, ok := profile.(*EventedProfile); ok {

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
			duration := eventedProfile.EndValue - eventedProfile.StartValue
			switch eventedProfile.Unit {
			case Bytes:
				// XXX can't calculate a duration from a non time based unit. What to do here?
				continue
			case Seconds:
				duration = duration * 1e9
			case Milliseconds:
				duration = duration * 1e6
			case Microseconds:
				duration = duration * 1e3
			}

			var at float64
			frameIdStack := make(int64Stack, 0) // current stack of indices into `frames`

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
						if frame.File != nil {
							if frameFields.maybeSetString("filename", *frame.File) {
								if *frame.Line > 0 {
									fields.set("line", frame.Line)
								}
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

			for i, evt := range eventedProfile.Events {
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
