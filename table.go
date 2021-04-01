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
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"

	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

// Table represents a Spanner table.
type Table struct {
	Name        string
	Columns     []string
	ChildTables []*Table
}

func (t *Table) String() string {
	return fmt.Sprintf("{Name: %q, Columns: %v, ChildTables: %v}", t.Name, t.Columns, t.ChildTables)
}

func (t *Table) quotedColumnList() string {
	var quoted []string
	for _, c := range t.Columns {
		quoted = append(quoted, fmt.Sprintf("`%s`", c))
	}
	return strings.Join(quoted, ", ")
}

// TableIterator is an iterator to get tables in the database one by one.
type TableIterator struct {
	tables []*Table
}

// Do executes a given func with each table in the database.
func (i *TableIterator) Do(f func(*Table) error) error {
	for _, t := range i.tables {
		if err := f(t); err != nil {
			return err
		}
		if len(t.ChildTables) > 0 {
			childIter := &TableIterator{t.ChildTables}
			if err := childIter.Do(f); err != nil {
				return err
			}
		}
	}
	return nil
}

type tableRow struct {
	name       string
	parentName string
	columns    []string
}

// FetchTables fetches all table information in the database from Spanner.
func FetchTables(ctx context.Context, txn *spanner.ReadOnlyTransaction) (*TableIterator, error) {
	// SQL for fetching table name, parent, and columns
	stmt := spanner.NewStatement(`
SELECT t.TABLE_NAME as table, t.PARENT_TABLE_NAME as parent, c.columns
FROM INFORMATION_SCHEMA.TABLES as t
JOIN (
    SELECT c.TABLE_NAME as table, ARRAY_AGG(c.COLUMN_NAME) as columns
    FROM INFORMATION_SCHEMA.COLUMNS AS c
    WHERE c.TABLE_CATALOG = '' AND c.TABLE_SCHEMA = ''
    GROUP BY c.TABLE_NAME
) as c
ON t.TABLE_NAME = c.table
WHERE t.TABLE_CATALOG = '' AND t.TABLE_SCHEMA = ''
ORDER BY t.TABLE_NAME ASC
`)
	var rows []tableRow
	opts := spanner.QueryOptions{Priority: sppb.RequestOptions_PRIORITY_LOW}
	if err := txn.QueryWithOptions(ctx, stmt, opts).Do(func(r *spanner.Row) error {
		var tableName, parentTableName string
		var columns []string
		var parentTableNamePtr *string // nullable

		if err := r.ColumnByName("table", &tableName); err != nil {
			return err
		}

		if err := r.ColumnByName("parent", &parentTableNamePtr); err != nil {
			return err
		}
		if parentTableNamePtr != nil {
			parentTableName = *parentTableNamePtr
		}

		if err := r.ColumnByName("columns", &columns); err != nil {
			return err
		}

		rows = append(rows, tableRow{
			name:       tableName,
			columns:    columns,
			parentName: parentTableName,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	tables := findChildTables(rows, "") // root
	return &TableIterator{tables}, nil
}

func findChildTables(rows []tableRow, parent string) []*Table {
	var tables []*Table
	for _, row := range rows {
		if row.parentName == parent {
			tables = append(tables, &Table{
				Name:        row.name,
				Columns:     row.columns,
				ChildTables: findChildTables(rows, row.name),
			})
		}
	}
	return tables
}
