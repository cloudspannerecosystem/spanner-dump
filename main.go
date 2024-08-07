package main

import (
	"context"
	"fmt"
	"github.com/jessevdk/go-flags"
	"os"
	"strings"
	"time"
)

type options struct {
	ProjectId  string `short:"p" long:"project" env:"SPANNER_PROJECT_ID" description:"(required) GCP Project ID."`
	InstanceId string `short:"i" long:"instance" env:"SPANNER_INSTANCE_ID" description:"(required) Cloud Spanner Instance ID."`
	DatabaseId string `short:"d" long:"database" env:"SPANNER_DATABASE_ID" description:"(required) Cloud Spanner Database ID."`
	Tables     string `long:"tables" description:"comma-separated table names, e.g. \"table1,table2\" "`
	NoDDL      bool   `long:"no-ddl" description:"No DDL information."`
	NoData     bool   `long:"no-data" description:"Do not dump data."`
	Timestamp  string `long:"timestamp" description:"Timestamp for database snapshot in the RFC 3339 format."`
	BulkSize   uint   `long:"bulk-size" description:"Bulk size for values in a single INSERT statement."`
	QuerySql   string `long:"where" description:"Query sql for values in a SElect statement."`
	Format     string `long:"format" description:"Format of the output, can be 'json' or 'sql'."`
}

func main() {
	var opts options

	if _, err := flags.Parse(&opts); err != nil {
		exitf("Invalid options\n")
	}

	if opts.ProjectId == "" || opts.InstanceId == "" || opts.DatabaseId == "" {
		exitf("Missing parameters: -p, -i, -d are required\n")
	}
	if opts.Format != "json" && opts.Format != "sql" {
		exitf("Invalid format: -format can be 'json' or 'sql'\n")
	}
	var timestamp *time.Time
	if opts.Timestamp != "" {
		t, err := time.Parse(time.RFC3339, opts.Timestamp)
		if err != nil {
			exitf("Failed to parse timestamp: %v\n", err)
		}
		timestamp = &t
	}

	var tables []string
	if opts.Tables != "" {
		tables = strings.Split(opts.Tables, ",")
	}

	ctx := context.Background()
	dumper, err := NewDumper(ctx, opts.ProjectId, opts.InstanceId, opts.DatabaseId, os.Stdout, timestamp, opts.BulkSize, tables, opts.QuerySql, opts.Format)
	if err != nil {
		exitf("Failed to create dumper: %v\n", err)
	}
	defer dumper.Cleanup()

	if !opts.NoDDL {
		if err := dumper.DumpDDLs(ctx); err != nil {
			exitf("Failed to dump DDLs: %v\n", err)
		}
	}

	if !opts.NoData {
		if err := dumper.DumpTables(ctx); err != nil {
			exitf("Failed to dump tables: %v\n", err)
		}
	}
}

func exitf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(1)
}
