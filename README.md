spanner-dump [![CircleCI](https://circleci.com/gh/cloudspannerecosystem/spanner-dump.svg?style=svg)](https://circleci.com/gh/cloudspannerecosystem/spanner-dump)
===

spanner-dump is a command line tool for exporting a Cloud Spanner database in text format.

Exported databases can be imported to Cloud Spanner with [spanner-cli](https://github.com/cloudspannerecosystem/spanner-cli).

```sh
# Export
$ spanner-dump -p ${PROJECT} -i ${INSTANCE} -d ${DATABASE} > data.sql

# Import
$ spanner-cli -p ${PROJECT} -i ${INSTANCE} -d ${DATABASE} < data.sql
```

Please feel free to report issues and send pull requests, but note that this application is not officially supported as part of the Cloud Spanner product.

## Use cases

This tool can be used for the following use cases.

- Export a database schema and/or data in text format for testing purposes
- Export a database running on [Cloud Spanner Emulator](https://cloud.google.com/spanner/docs/emulator)

For production databases, you should use an [officially-provided export](https://cloud.google.com/spanner/docs/export),
which should be stable and much faster.

## Limitations

- This tool does not ensure consistency between database schema (DDL) and data. So you should avoid making changes to the schema while you are running this tool. 
- This tool does not consider data order constrained by [Foreign Keys](https://cloud.google.com/spanner/docs/foreign-keys/overview).

## Install

```
go get -u github.com/cloudspannerecosystem/spanner-dump
```

## Usage

```
Usage:
  spanner-dump [OPTIONS]

Application Options:
  -p, --project=   (required) GCP Project ID.
  -i, --instance=  (required) Cloud Spanner Instance ID.
  -d, --database=  (required) Cloud Spanner Database ID.
      --tables=    comma-separated table names, e.g. "table1,table2"
      --no-ddl     No DDL information.
      --no-data    Do not dump data.
      --timestamp= Timestamp for database snapshot in the RFC 3339 format.
      --bulk-size= Bulk size for values in a single INSERT statement.

Help Options:
  -h, --help       Show this help message
```

This tool uses [Application Default Credentials](https://cloud.google.com/docs/authentication/production)
to connect to Cloud Spanner. Please make sure to get credentials via `gcloud auth application-default login`
before using this tool.

Also, you need to have a [roles/spanner.databaseReader](https://cloud.google.com/spanner/docs/iam#roles)
IAM role to use this tool.

## Disclaimer
This tool is still ALPHA quality. Do not use this tool for production databases.
