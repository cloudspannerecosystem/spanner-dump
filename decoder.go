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
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	pb "google.golang.org/genproto/googleapis/spanner/v1"
)

// DecodeRow decodes column values in spanner.Row into strings.
func DecodeRow(row *spanner.Row) ([]string, error) {
	columns := make([]string, row.Size())
	for i := 0; i < row.Size(); i++ {
		var column spanner.GenericColumnValue
		if err := row.Column(i, &column); err != nil {
			return nil, err
		}
		decoded, err := DecodeColumn(column)
		if err != nil {
			return nil, err
		}
		columns[i] = decoded
	}
	return columns, nil
}

// DecodeColumn decodes a single column value into a string.
func DecodeColumn(column spanner.GenericColumnValue) (string, error) {
	// Note that STRUCT data type is not supported as it's not allowed for column types.
	// See: https://cloud.google.com/spanner/docs/data-types#allowable-types
	switch column.Type.Code {
	case pb.TypeCode_ARRAY:
		var decoded []string
		switch column.Type.GetArrayElementType().Code {
		case pb.TypeCode_BOOL:
			var vs []spanner.NullBool
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullBoolToString(v))
			}
		case pb.TypeCode_BYTES:
			var vs [][]byte
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullBytesToString(v))
			}
		case pb.TypeCode_FLOAT64:
			var vs []spanner.NullFloat64
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullFloat64ToString(v))
			}
		case pb.TypeCode_INT64:
			var vs []spanner.NullInt64
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullInt64ToString(v))
			}
		case pb.TypeCode_STRING:
			var vs []spanner.NullString
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullStringToString(v))
			}
		case pb.TypeCode_TIMESTAMP:
			var vs []spanner.NullTime
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullTimeToString(v))
			}
		case pb.TypeCode_DATE:
			var vs []spanner.NullDate
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullDateToString(v))
			}
		case pb.TypeCode_STRUCT:
			return "", errors.New("unexpected error: column has STRUCT data type")
		}
		return fmt.Sprintf("[%s]", strings.Join(decoded, ", ")), nil
	case pb.TypeCode_BOOL:
		var v spanner.NullBool
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullBoolToString(v), nil
	case pb.TypeCode_BYTES:
		var v []byte
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullBytesToString(v), nil
	case pb.TypeCode_FLOAT64:
		var v spanner.NullFloat64
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullFloat64ToString(v), nil
	case pb.TypeCode_INT64:
		var v spanner.NullInt64
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullInt64ToString(v), nil
	case pb.TypeCode_STRING:
		var v spanner.NullString
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullStringToString(v), nil
	case pb.TypeCode_TIMESTAMP:
		var v spanner.NullTime
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullTimeToString(v), nil
	case pb.TypeCode_DATE:
		var v spanner.NullDate
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullDateToString(v), nil
	default:
		return fmt.Sprintf("%s", column.Value), nil
	}
}

func nullBoolToString(v spanner.NullBool) string {
	if v.Valid {
		return fmt.Sprintf("%t", v.Bool)
	} else {
		return "NULL"
	}
}

func nullBytesToString(v []byte) string {
	if v == nil {
		return "NULL"
	}

	// Converts []byte to bytes literal like b"\xc2\xa9"
	var sb strings.Builder
	sb.WriteString("b") // prefix for bytes literal
	sb.WriteString(`"`) // quote start
	for _, b := range v {
		sb.WriteString(fmt.Sprintf("\\x%x", b))
	}
	sb.WriteString(`"`) // quote end
	return sb.String()
}

func nullFloat64ToString(v spanner.NullFloat64) string {
	switch {
	case !v.Valid:
		return "NULL"
	case math.IsNaN(v.Float64):
		return "CAST('nan' AS FLOAT64)"
	case math.IsInf(v.Float64, 1):
		return "CAST('inf' AS FLOAT64)"
	case math.IsInf(v.Float64, -1):
		return "CAST('-inf' AS FLOAT64)"
	default:
		return strconv.FormatFloat(v.Float64, 'g', -1, 64)
	}
}

func nullInt64ToString(v spanner.NullInt64) string {
	if v.Valid {
		return fmt.Sprintf("%d", v.Int64)
	} else {
		return "NULL"
	}
}

func nullStringToString(v spanner.NullString) string {
	if v.Valid {
		return strconv.Quote(v.StringVal)
	} else {
		return "NULL"
	}
}

func nullTimeToString(v spanner.NullTime) string {
	if v.Valid {
		// Timestamp Literal: https://cloud.google.com/spanner/docs/lexical#timestamp_literals
		return fmt.Sprintf(`TIMESTAMP "%s"`, v.Time.Format(time.RFC3339Nano))
	} else {
		return "NULL"
	}
}

func nullDateToString(v spanner.NullDate) string {
	if v.Valid {
		// Date Literal: https://cloud.google.com/spanner/docs/lexical#date_literals
		return fmt.Sprintf(`DATE "%s"`, v.Date.String())
	} else {
		return "NULL"
	}
}
