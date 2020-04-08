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
	"testing"
)

func TestQuotedColumnList(t *testing.T) {
	for _, tt := range []struct{
		desc string
		table *Table
		want string
	}{
		{
			desc: "No columns",
			table: &Table{Columns: []string{}},
			want: "",
		},
		{
			desc: "Single column",
			table: &Table{Columns: []string{"C1"}},
			want: "`C1`",
		},
		{
			desc: "Multiple columns",
			table: &Table{Columns: []string{"C1", "C2"}},
			want: "`C1`, `C2`",
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			if got := tt.table.quotedColumnList(); got != tt.want {
				t.Errorf("quotedColumnList() of %v: got = %v, want = %v", tt.table, got, tt.want)
			}
		})
	}

}

func TestFindChildTables(t *testing.T) {
	// Table Tree:
	// (Root)
	//    |- T1
	//    |   |- T2
	//    |   |- T3
	//    |       |- T4
	//    |- T5
	tr1 := tableRow{name: "T1", parentName: "", columns: []string{"T1_C1", "T1_C2"}}
	tr2 := tableRow{name: "T2", parentName: "T1", columns: []string{"T2_C1", "T2_C2"}}
	tr3 := tableRow{name: "T3", parentName: "T1", columns: []string{"T3_C1", "T3_C2"}}
	tr4 := tableRow{name: "T4", parentName: "T3", columns: []string{"T4_C1", "T4_C2"}}
	tr5 := tableRow{name: "T5", parentName: "", columns: []string{"T5_C1", "T5_C2"}}

	t4 := &Table{Name: "T4", Columns: []string{"T4_C1", "T4_C2"}, ChildTables: nil}
	t2 := &Table{Name: "T2", Columns: []string{"T2_C1", "T2_C2"}, ChildTables: nil}
	t5 := &Table{Name: "T5", Columns: []string{"T5_C1", "T5_C2"}, ChildTables: nil}
	t3 := &Table{Name: "T3", Columns: []string{"T3_C1", "T3_C2"}, ChildTables: []*Table{t4}}
	t1 := &Table{Name: "T1", Columns: []string{"T1_C1", "T1_C2"}, ChildTables: []*Table{t2, t3}}

	for _, tt := range []struct{
		desc string
		rows []tableRow
		parent string
		want []*Table
	}{
		{
			desc: "No rows",
			rows: []tableRow{},
			parent: "",
			want: []*Table{},
		},
		{
			desc: "Middle tree",
			rows: []tableRow{tr2, tr3, tr4},
			parent: "T1",
			want: []*Table{t2, t3},
		},
		{
			desc: "Full tree",
			rows: []tableRow{tr1, tr2, tr3, tr4, tr5},
			parent: "",
			want: []*Table{t1, t5},
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			got := findChildTables(tt.rows, tt.parent)
			if !equalsTables(got, tt.want) {
				t.Errorf("findChildTables(%v, %q) = %v, want = %v", tt.rows, tt.parent, got, tt.want)
			}
		})
	}
}

func equalsTables(tables1, tables2 []*Table) bool {
	if len(tables1) != len(tables2) {
		return false
	}

	for i := 0; i < len(tables1); i++ {
		if !equalsTable(tables1[i], tables2[i]) {
			return false
		}
	}

	return true
}

func equalsTable(t1, t2 *Table) bool {
	if t1 == nil && t2 == nil {
		return true
	}

	if t1 == nil || t2 == nil {
		if t1 == nil && t2 != nil {
			return false
		}
		if t1 != nil && t2 == nil {
			return false
		}
	}

	if t1.Name != t2.Name {
		return false
	}
	if len(t1.Columns) != len(t2.Columns) {
		return false
	}
	for i := 0; i < len(t1.Columns); i++ {
		if t1.Columns[i] != t2.Columns[i] {
			return false
		}
	}

	return equalsTables(t1.ChildTables, t2.ChildTables)
}
