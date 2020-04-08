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
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
	"io"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"

	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

// This is an ad hoc value, but considering mutations limit (20,000),
// 100 rows/statement would be safe in most cases.
// https://cloud.google.com/spanner/quotas#limits_for_creating_reading_updating_and_deleting_data
const defaultBulkSize = 100

// Dumper is a dumper to export a database.
type Dumper struct {
	project   string
	instance  string
	database  string
	out       io.Writer
	timestamp *time.Time
	bulkSize  uint

	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient
}

// NewDumper creates Dumper with specified configurations.
func NewDumper(ctx context.Context, project, instance, database string, out io.Writer, timestamp *time.Time, bulkSize uint) (*Dumper, error) {
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)
	client, err := spanner.NewClientWithConfig(ctx, dbPath, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened: 1,
			MaxOpened: 1,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create spanner client: %v", err)
	}

	var opts []option.ClientOption
	if emulatorAddr := os.Getenv("SPANNER_EMULATOR_HOST"); emulatorAddr != "" {
		emulatorOpts := []option.ClientOption{
			option.WithEndpoint(emulatorAddr),
			option.WithGRPCDialOption(grpc.WithInsecure()),
			option.WithoutAuthentication(),
		}
		opts = append(opts, emulatorOpts...)
	}
	adminClient, err := adminapi.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create spanner admin client: %v", err)
	}

	if bulkSize == 0 {
		bulkSize = defaultBulkSize
	}

	return &Dumper{
		project:     project,
		instance:    instance,
		database:    database,
		out:         out,
		timestamp:   timestamp,
		bulkSize:    bulkSize,
		client:      client,
		adminClient: adminClient,
	}, nil
}

// Cleanup cleans up hold resources.
func (d *Dumper) Cleanup() {
	d.client.Close()
	d.adminClient.Close()
}

// DumpDDLs dumps all DDLs in the database.
func (d *Dumper) DumpDDLs(ctx context.Context) error {
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", d.project, d.instance, d.database)
	resp, err := d.adminClient.GetDatabaseDdl(ctx, &adminpb.GetDatabaseDdlRequest{
		Database: dbPath,
	})
	if err != nil {
		return err
	}

	for _, ddl := range resp.Statements {
		fmt.Fprintf(d.out, "%s;\n", ddl)
	}

	return nil
}

// DumpTables dumps all table records in the database.
func (d *Dumper) DumpTables(ctx context.Context) error {
	txn := d.client.ReadOnlyTransaction()
	if d.timestamp != nil {
		txn = txn.WithTimestampBound(spanner.ReadTimestamp(*d.timestamp))
	}

	iter, err := FetchTables(ctx, txn)
	if err != nil {
		return err
	}

	return iter.Do(func (t *Table) error {
		return d.dumpTable(ctx, t, txn)
	})
}

func (d *Dumper) dumpTable(ctx context.Context, table *Table, txn *spanner.ReadOnlyTransaction) error {
	stmt := spanner.NewStatement(fmt.Sprintf("SELECT %s FROM `%s`", table.quotedColumnList(), table.Name))
	iter := txn.Query(ctx, stmt)
	defer iter.Stop()

	writer := NewBufferedWriter(table, d.out, d.bulkSize)
	defer writer.Flush()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		values, err := DecodeRow(row)
		if err != nil {
			return err
		}
		writer.Write(values)
	}

	return nil
}
