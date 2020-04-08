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
	"bytes"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"

	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const (
	envTestProjectId  = "SPANNER_DUMP_INTEGRATION_TEST_PROJECT_ID"
	envTestInstanceId = "SPANNER_DUMP_INTEGRATION_TEST_INSTANCE_ID"
)

var (
	skipIntegrateTest bool

	testProjectId  string
	testInstanceId string

	tableIdCounter uint32
)

func TestMain(m *testing.M) {
	initialize()
	os.Exit(m.Run())
}

func initialize() {
	if os.Getenv(envTestProjectId) == "" || os.Getenv(envTestInstanceId) == "" {
		skipIntegrateTest = true
		return
	}

	testProjectId = os.Getenv(envTestProjectId)
	testInstanceId = os.Getenv(envTestInstanceId)
}

func generateUniqueDatabaseId() string {
	count := atomic.AddUint32(&tableIdCounter, 1)
	return fmt.Sprintf("spanner_dump_test_%d_%d", time.Now().Unix(), count)
}

func setup(t *testing.T, ctx context.Context, ddls, dmls []string) (string, func()) {
	databaseId := generateUniqueDatabaseId()

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
		t.Fatalf("failed to create spanner admin client: %v", err)
	}

	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent: fmt.Sprintf("projects/%s/instances/%s", testProjectId, testInstanceId),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseId),
		ExtraStatements: ddls,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	db, err := op.Wait(ctx)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	client, err := spanner.NewClientWithConfig(ctx, db.Name, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened: 1,
			MaxOpened: 1,
		},
	})
	if err != nil {
		t.Fatalf("failed to create spanner client: %v", err)
	}

	for _, dml := range dmls {
		_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			_, err = txn.Update(ctx, spanner.NewStatement(dml))
			return err
		})
		if err != nil {
			t.Fatalf("failed to apply DML %q: %v", dml, err)
		}
	}

	tearDown := func() {
		if err = adminClient.DropDatabase(ctx, &adminpb.DropDatabaseRequest{
			Database:   db.Name,
		}); err != nil {
			t.Fatalf("failed to drop database: %v", err)
		}
	}

	return databaseId, tearDown
}

func TestDump(t *testing.T) {
	if skipIntegrateTest {
		t.Skip("skip integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// NOTE: Spanner doesn't allow to use trailer ";" in DDL.
	ddls := []string{
`CREATE TABLE t1 (
  Id INT64 NOT NULL,
  StrCol STRING(16),
  BoolCol BOOL,
  BytesCol BYTES(16),
  TimestampCol TIMESTAMP,
  DateCol DATE,
  ArrayCol ARRAY<INT64>,
) PRIMARY KEY(Id)`,

`CREATE TABLE t2 (
  T2Id INT64 NOT NULL,
) PRIMARY KEY(T2Id)`,

`CREATE TABLE t3 (
  T2Id INT64 NOT NULL,
  T3Id INT64 NOT NULL,
) PRIMARY KEY(T2Id, T3Id),
  INTERLEAVE IN PARENT t2 ON DELETE CASCADE`,

`CREATE TABLE t4 (
  T2Id INT64 NOT NULL,
  T3Id INT64 NOT NULL,
  T4Id INT64 NOT NULL,
) PRIMARY KEY(T2Id, T3Id, T4Id),
  INTERLEAVE IN PARENT t3 ON DELETE CASCADE`,
	}

	dmls := []string{
		"INSERT INTO `t1` (`Id`, `StrCol`, `BoolCol`, `BytesCol`, `TimestampCol`, `DateCol`, `ArrayCol`) VALUES (1, \"foo\", true, b\"\\x61\\x62\\x63\", TIMESTAMP \"2020-01-23T03:00:00Z\", DATE \"2020-01-23\", [1, 2, 3]);",
		"INSERT INTO `t1` (`Id`, `StrCol`, `BoolCol`, `BytesCol`, `TimestampCol`, `DateCol`, `ArrayCol`) VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL);",
		"INSERT INTO `t2` (`T2Id`) VALUES (1);",
		"INSERT INTO `t2` (`T2Id`) VALUES (2);",
		"INSERT INTO `t2` (`T2Id`) VALUES (3);",
		"INSERT INTO `t3` (`T2Id`, `T3Id`) VALUES (1, 1);",
		"INSERT INTO `t3` (`T2Id`, `T3Id`) VALUES (2, 2);",
		"INSERT INTO `t3` (`T2Id`, `T3Id`) VALUES (3, 3);",
		"INSERT INTO `t4` (`T2Id`, `T3Id`, `T4Id`) VALUES (1, 1, 1);",
		"INSERT INTO `t4` (`T2Id`, `T3Id`, `T4Id`) VALUES (2, 2, 2);",
		"INSERT INTO `t4` (`T2Id`, `T3Id`, `T4Id`) VALUES (3, 3, 3);",
	}
	databaseId, tearDown := setup(t, ctx, ddls, dmls)
	defer tearDown()

	out := &bytes.Buffer{}
	dumper, err := NewDumper(ctx, testProjectId, testInstanceId, databaseId, out, nil, 1)
	if err != nil {
		t.Fatalf("failed to create dumper: %v", err)
	}

	if err := dumper.DumpDDLs(ctx); err != nil {
		t.Fatalf("failed to dump DDLs: %v", err)
	}

	got := out.String()
	want := strings.Join(ddls, ";\n") + ";\n"
	if got != want {
		t.Errorf("DumpDDLs() = %q, but want = %q", got, want)
	}

	out.Reset()
	if err := dumper.DumpTables(ctx); err != nil {
		t.Fatalf("failed to dump tables: %v", err)
	}
	got = out.String()
	want = strings.Join(dmls, "\n") + "\n"
	if got != want {
		t.Errorf("DumpTables() = %q, but want = %q", got, want)
	}
}
