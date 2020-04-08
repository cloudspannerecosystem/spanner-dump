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

// spanner-dump is a command line tool for exporting a Cloud Spanner database in text format.
package main

import (
	"context"
	"fmt"
	"github.com/jessevdk/go-flags"
	"os"
	"time"
)

type options struct {
	ProjectId  string `short:"p" long:"project" description:"(required) GCP Project ID."`
	InstanceId string `short:"i" long:"instance" description:"(required) Cloud Spanner Instance ID."`
	DatabaseId string `short:"d" long:"database" description:"(required) Cloud Spanner Database ID."`
	NoDDL      bool   `long:"no-ddl" description:"No DDL information."`
	Timestamp  string `long:"timestamp" description:"Timestamp for database snapshot in the RFC 3339 format."`
	BulkSize   uint   `long:"bulk-size" description:"Bulk size for values in a single INSERT statement."`
}

func main() {
	var opts options
	if _, err := flags.Parse(&opts); err != nil {
		exitf("Invalid options\n")
	}

	if opts.ProjectId == "" || opts.InstanceId == "" || opts.DatabaseId == "" {
		exitf("Missing parameters: -p, -i, -d are required\n")
	}

	var timestamp *time.Time
	if opts.Timestamp != "" {
		t, err := time.Parse(time.RFC3339, opts.Timestamp)
		if err != nil {
			exitf("Failed to parse timestamp: %v\n", err)
		}
		timestamp = &t
	}

	ctx := context.Background()
	dumper, err := NewDumper(ctx, opts.ProjectId, opts.InstanceId, opts.DatabaseId, os.Stdout, timestamp, opts.BulkSize)
	if err != nil {
		exitf("Failed to create dumper: %v\n", err)
	}
	defer dumper.Cleanup()

	if !opts.NoDDL {
		if err := dumper.DumpDDLs(ctx); err != nil {
			exitf("Failed to dump DDLs: %v\n", err)
		}
	}

	if err := dumper.DumpTables(ctx); err != nil {
		exitf("Failed to dump tables: %v\n", err)
	}
}

func exitf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(1)
}
