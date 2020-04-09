//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"math"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
)

func createRow(t *testing.T, values []interface{}) *spanner.Row {
	t.Helper()

	// column names are not important in this test, so use dummy name
	names := make([]string, len(values))
	for i := 0; i < len(names); i++ {
		names[i] = "dummy"
	}

	row, err := spanner.NewRow(names, values)
	if err != nil {
		t.Fatalf("Creating spanner row failed unexpectedly: %v", err)
	}
	return row
}

func createColumnValue(t *testing.T, value interface{}) spanner.GenericColumnValue {
	t.Helper()

	row := createRow(t, []interface{}{value})
	var cv spanner.GenericColumnValue
	if err := row.Column(0, &cv); err != nil {
		t.Fatalf("Creating spanner column value failed unexpectedly: %v", err)
	}

	return cv
}

func equalStringSlice(a []string, b []string) bool {
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestDecodeColumn(t *testing.T) {
	for _, tt := range []struct {
		desc  string
		value interface{}
		want  string
	}{
		// non-nullable
		{
			desc:  "bool",
			value: true,
			want:  "true",
		},
		{
			desc:  "bytes",
			value: []byte{'a', 'b', 'c'},
			want:  `b"\x61\x62\x63"`,
		},
		{
			desc:  "float64",
			value: 1.23,
			want:  "1.23",
		},
		{
			desc:  "math.MaxFloat64",
			value: math.MaxFloat64,
			want:  "1.7976931348623157e+308",
		},
		{
			desc:  "-math.MaxFloat64",
			value: -math.MaxFloat64,
			want:  "-1.7976931348623157e+308",
		},
		{
			desc:  "math.SmallestNonzeroFloat64",
			value: math.SmallestNonzeroFloat64,
			want:  "5e-324",
		},
		{
			desc:  "-math.SmallestNonzeroFloat64",
			value: -math.SmallestNonzeroFloat64,
			want:  "-5e-324",
		},
		{
			desc:  "NaN",
			value: math.NaN(),
			want:  "CAST('nan' AS FLOAT64)",
		},
		{
			desc:  "Inf",
			value: math.Inf(+1),
			want:  "CAST('inf' AS FLOAT64)",
		},
		{
			desc:  "-Inf",
			value: math.Inf(-1),
			want:  "CAST('-inf' AS FLOAT64)",
		},
		{
			desc:  "int64",
			value: 123,
			want:  "123",
		},
		{
			desc:  "string",
			value: "foo",
			want:  `"foo"`,
		},
		{
			desc:  "string with double-quote",
			value: `foo"bar`,
			want:  `"foo\"bar"`,
		},
		{
			desc:  "string with new line",
			value: "foo\nbar",
			want:  `"foo\nbar"`,
		},
		{
			desc:  "timestamp",
			value: time.Unix(1516676400, 0),
			want:  `TIMESTAMP "2018-01-23T03:00:00Z"`,
		},
		{
			desc:  "date",
			value: civil.DateOf(time.Unix(1516676400, 0)),
			want:  `DATE "2018-01-23"`,
		},

		// nullable
		{
			desc:  "null bool",
			value: spanner.NullBool{Bool: false, Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null bytes",
			value: []byte(nil),
			want:  "NULL",
		},
		{
			desc:  "null float64",
			value: spanner.NullFloat64{Float64: 0, Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null int64",
			value: spanner.NullInt64{Int64: 0, Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null string",
			value: spanner.NullString{StringVal: "", Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null time",
			value: spanner.NullTime{Time: time.Unix(0, 0), Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null date",
			value: spanner.NullDate{Date: civil.DateOf(time.Unix(0, 0)), Valid: false},
			want:  "NULL",
		},

		// array non-nullable
		{
			desc:  "empty array",
			value: []bool{},
			want:  "[]",
		},
		{
			desc:  "array bool",
			value: []bool{true, false},
			want:  "[true, false]",
		},
		{
			desc:  "array bytes",
			value: [][]byte{{'a', 'b', 'c'}, {'d', 'e', 'f'}},
			want:  `[b"\x61\x62\x63", b"\x64\x65\x66"]`,
		},
		{
			desc:  "array float64",
			value: []float64{1.23, 2.45},
			want:  "[1.23, 2.45]",
		},
		{
			desc:  "array int64",
			value: []int64{123, 456},
			want:  "[123, 456]",
		},
		{
			desc:  "array string",
			value: []string{"foo", "bar"},
			want:  `["foo", "bar"]`,
		},
		{
			desc:  "array timestamp",
			value: []time.Time{time.Unix(1516676400, 0), time.Unix(1516680000, 0)},
			want:  `[TIMESTAMP "2018-01-23T03:00:00Z", TIMESTAMP "2018-01-23T04:00:00Z"]`,
		},
		{
			desc:  "array date",
			value: []civil.Date{civil.DateOf(time.Unix(1516676400, 0)), civil.DateOf(time.Unix(1516762800, 0))},
			want:  `[DATE "2018-01-23", DATE "2018-01-24"]`,
		},

		// array nullable
		{
			desc:  "null array bool",
			value: []bool(nil),
			want:  "NULL",
		},
		{
			desc:  "null array bytes",
			value: [][]byte(nil),
			want:  "NULL",
		},
		{
			desc:  "nul array float64",
			value: []float64(nil),
			want:  "NULL",
		},
		{
			desc:  "null array int64",
			value: []int64(nil),
			want:  "NULL",
		},
		{
			desc:  "null array string",
			value: []string(nil),
			want:  "NULL",
		},
		{
			desc:  "null array timestamp",
			value: []time.Time(nil),
			want:  "NULL",
		},
		{
			desc:  "null array date",
			value: []civil.Date(nil),
			want:  "NULL",
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			got, err := DecodeColumn(createColumnValue(t, tt.value))
			if err != nil {
				t.Error(err)
			}
			if got != tt.want {
				t.Errorf("DecodeColumn(%v) = %q, want = %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestDecodeColumn_roundtripFloat64(t *testing.T) {
	for _, tt := range []float64{
		math.MaxFloat64,
		-math.MaxFloat64,
		math.SmallestNonzeroFloat64,
		-math.SmallestNonzeroFloat64,
	} {
		s, err := DecodeColumn(createColumnValue(t, spanner.NullFloat64{Valid: true, Float64: tt}))
		if err != nil {
			t.Error(err)
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Error(err)
		}
		if f != tt {
			t.Errorf("expected: %g, actual: %g\n", tt, f)
		}
	}
}
func TestDecodeRow(t *testing.T) {
	for _, tt := range []struct {
		desc   string
		values []interface{}
		want   []string
	}{
		{
			desc:   "non-null columns",
			values: []interface{}{"foo", 123},
			want:   []string{`"foo"`, "123"},
		},
		{
			desc:   "non-null column and null column",
			values: []interface{}{"foo", spanner.NullString{StringVal: "", Valid: false}},
			want:   []string{`"foo"`, "NULL"},
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			got, err := DecodeRow(createRow(t, tt.values))
			if err != nil {
				t.Error(err)
			}
			if !equalStringSlice(got, tt.want) {
				t.Errorf("DecodeRow(%v) = %v, want = %v", tt.values, got, tt.want)
			}
		})
	}
}
