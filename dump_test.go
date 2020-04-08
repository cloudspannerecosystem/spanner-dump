package main

import "testing"

func Test_parsesTableName(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{
			name: "CREATE TABLE",
			statement: `CREATE TABLE table_name_1 (
  column1 STRING(32) NOT NULL,
  column2 TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY(column1);`,
			want: "table_name_1",
		},
		{
			name:      "CREATE UNIQUE INDEX",
			statement: `CREATE UNIQUE INDEX table_name_1_column2_a ON table_name_1(column2);`,
			want:      "table_name_1",
		},
		{
			name:      "CREATE INDEX",
			statement: `CREATE INDEX table_name_1_column2_a ON table_name_1(column2);`,
			want:      "table_name_1",
		},
		{
			name:      "CREATE INDEX",
			statement: `CREATE INDEX table_name_1_column2_a ON table_name_1(column2);`,
			want:      "table_name_1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parsesTableName(tt.statement); got != tt.want {
				t.Errorf("parsesTableName() = %v, want %v", got, tt.want)
			}
		})
	}
}
