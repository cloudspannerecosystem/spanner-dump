package main

import "testing"

func TestParseTableNameFromDDL(t *testing.T) {
	tests := []struct {
		name string
		ddl  string
		want string
	}{
		{
			name: "create table",
			ddl: `CREATE TABLE table_name_1 (
  column1 STRING(32) NOT NULL,
  column2 TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY(column1);`,
			want: "table_name_1",
		},
		{
			name: "create table, table include reserved words",
			ddl: `CREATE TABLE ` + "`table_name_1`" + ` (
  column1 STRING(32) NOT NULL,
  column2 TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY(column1);`,
			want: "table_name_1",
		},
		{
			name: "create unique index",
			ddl:  `CREATE UNIQUE INDEX table_name_1_column2_a ON table_name_1(column2);`,
			want: "table_name_1",
		},
		{
			name: "create index",
			ddl:  `CREATE INDEX table_name_1_column2_a ON table_name_1(column2);`,
			want: "table_name_1",
		},
		{
			name: "index name include reserved words",
			ddl:  "CREATE INDEX `order` ON TABLE(`by`)",
			want: "TABLE",
		},
		{
			name: "table name reserved words",
			ddl:  "CREATE INDEX `order` ON `TABLE`(`by`)",
			want: "TABLE",
		},
		{
			name: "include multiple spaces",
			ddl:  "  CREATE   INDEX    `order`   ON    TABLE(`by`)",
			want: "TABLE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseTableNameFromDDL(tt.ddl); got != tt.want {
				t.Errorf("parseTableNameFromDDL() = %v, want %v", got, tt.want)
			}
		})
	}
}
