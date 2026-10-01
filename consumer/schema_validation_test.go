package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestPostgresSchemaValidatorCompatibility(t *testing.T) {
	source := openTestDatabase(t)
	destination := openDestinationDatabase(t)
	assertDistinctDatabases(t, source, destination)

	tests := []struct {
		name           string
		sourceDDL      string
		destinationDDL string
		wantStatus     compatibilityStatus
		wantDifference schemaDifferenceType
		wantColumn     string
	}{
		{
			name: "schemas identicos",
			sourceDDL: `(
				id BIGINT PRIMARY KEY,
				nome TEXT NOT NULL,
				email TEXT
			)`,
			destinationDDL: `(
				id BIGINT PRIMARY KEY,
				nome TEXT NOT NULL,
				email TEXT
			)`,
			wantStatus: compatibilityCompatible,
		},
		{
			name: "coluna ausente no destination",
			sourceDDL: `(
				id BIGINT PRIMARY KEY,
				nome TEXT NOT NULL,
				email TEXT,
				telefone TEXT
			)`,
			destinationDDL: `(
				id BIGINT PRIMARY KEY,
				nome TEXT NOT NULL,
				email TEXT
			)`,
			wantStatus:     compatibilityIncompatible,
			wantDifference: differenceColumnMissing,
			wantColumn:     "telefone",
		},
		{
			name: "coluna extra nullable no destination",
			sourceDDL: `(
				id BIGINT PRIMARY KEY,
				nome TEXT NOT NULL,
				email TEXT
			)`,
			destinationDDL: `(
				id BIGINT PRIMARY KEY,
				nome TEXT NOT NULL,
				email TEXT,
				telefone TEXT
			)`,
			wantStatus: compatibilityCompatible,
		},
		{
			name: "tipo incompatível",
			sourceDDL: `(
				id BIGINT PRIMARY KEY,
				idade INTEGER
			)`,
			destinationDDL: `(
				id BIGINT PRIMARY KEY,
				idade TEXT
			)`,
			wantStatus:     compatibilityIncompatible,
			wantDifference: differenceTypeMismatch,
			wantColumn:     "idade",
		},
		{
			name: "nulabilidade incompatível",
			sourceDDL: `(
				id BIGINT PRIMARY KEY,
				email TEXT
			)`,
			destinationDDL: `(
				id BIGINT PRIMARY KEY,
				email TEXT NOT NULL
			)`,
			wantStatus:     compatibilityIncompatible,
			wantDifference: differenceNullabilityMismatch,
			wantColumn:     "email",
		},
		{
			name: "chave primaria em ordem diferente",
			sourceDDL: `(
				tenant_id BIGINT NOT NULL,
				id BIGINT NOT NULL,
				descricao TEXT,
				PRIMARY KEY (tenant_id, id)
			)`,
			destinationDDL: `(
				tenant_id BIGINT NOT NULL,
				id BIGINT NOT NULL,
				descricao TEXT,
				PRIMARY KEY (id, tenant_id)
			)`,
			wantStatus:     compatibilityIncompatible,
			wantDifference: differencePrimaryKeyMismatch,
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := createSchemaValidationTables(t, source, destination, index, test.sourceDDL, test.destinationDDL)
			validator, err := newPostgresSchemaValidator(context.Background(), cloneDestinationConfig(t, source), cloneDestinationConfig(t, destination), []qualifiedTable{table})
			if err != nil {
				t.Fatalf("criar validator: %v", err)
			}
			defer validator.Close(context.Background())

			result := validator.Validate(context.Background(), table, pglogrepl.LSN(0x900+index), uint32(100+index))
			if result.Status != test.wantStatus {
				t.Fatalf("status inesperado: esperado=%s atual=%s diff=%+v", test.wantStatus, result.Status, result.Differences)
			}
			if result.Schema != table.schema || result.Table != table.name || result.LSN == 0 || result.TransactionID == 0 {
				t.Fatalf("resultado sem contexto preservado: %+v", result)
			}
			if test.wantDifference != "" && !hasSchemaDifference(result, test.wantDifference, test.wantColumn) {
				t.Fatalf("diferenca %s/%s nao encontrada em %+v", test.wantDifference, test.wantColumn, result.Differences)
			}
		})
	}
}

func TestPostgresSchemaValidatorReportsMissingDestinationTable(t *testing.T) {
	source := openTestDatabase(t)
	destination := openDestinationDatabase(t)
	assertDistinctDatabases(t, source, destination)

	tableName := "cdc_schema_validation_missing_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	table := qualifiedTable{schema: "public", name: tableName}
	tableSQL := pgx.Identifier{table.schema, table.name}.Sanitize()
	execSQL(t, source, "CREATE TABLE "+tableSQL+" (id BIGINT PRIMARY KEY)")
	t.Cleanup(func() {
		execSQL(t, source, "DROP TABLE IF EXISTS "+tableSQL)
		execSQL(t, destination, "DROP TABLE IF EXISTS "+tableSQL)
	})

	validator, err := newPostgresSchemaValidator(context.Background(), cloneDestinationConfig(t, source), cloneDestinationConfig(t, destination), []qualifiedTable{table})
	if err != nil {
		t.Fatalf("criar validator: %v", err)
	}
	defer validator.Close(context.Background())

	result := validator.Validate(context.Background(), table, 0x950, 200)
	if result.Status != compatibilityIncompatible || !hasSchemaDifference(result, differenceTableMissing, "") {
		t.Fatalf("tabela ausente deveria ser INCOMPATIBLE/TABLE_MISSING: %+v", result)
	}
}

func TestPostgresSchemaValidatorRespectsIncludedTables(t *testing.T) {
	validator := &postgresSchemaValidator{
		includedTables: map[string]struct{}{"public.clientes": {}},
	}
	if !validator.ShouldValidate(qualifiedTable{schema: "public", name: "clientes"}) {
		t.Fatal("tabela inclusa deveria ser validada")
	}
	if validator.ShouldValidate(qualifiedTable{schema: "public", name: "enderecos"}) {
		t.Fatal("tabela fora de CDC_TABLE_INCLUDE nao deveria ser validada")
	}
}

func TestSchemaValidationColumnMissingPolicy(t *testing.T) {
	table := qualifiedTable{schema: "public", name: "clientes"}
	result := schemaValidationResult{
		Status:        compatibilityIncompatible,
		Schema:        table.schema,
		Table:         table.name,
		LSN:           0x900,
		TransactionID: 7,
		SourceSchema: tableSchema{Schema: table.schema, Table: table.name, Columns: []columnSchema{
			{Name: "telefone", DataType: "text", TypeOID: pgtype.TextOID, Typmod: -1, Nullable: true},
		}},
		DestinationSchema: tableSchema{Schema: table.schema, Table: table.name},
		Differences:       []schemaDifference{{Type: differenceColumnMissing, Column: "telefone", Reason: "source column is absent in destination"}},
	}

	sink := &postgresSink{applyMode: "generic", schemaEvolution: "auto"}
	changes, err := sink.schemaValidationApplyCandidates(result)
	if err != nil {
		t.Fatalf("ADD COLUMN nullable deveria gerar candidato controlado: %v", err)
	}
	if len(changes) != 1 || !changes[0].HasColumn || changes[0].Column.Name != "telefone" {
		t.Fatalf("candidato inesperado: %+v", changes)
	}

	sink.schemaEvolution = "disabled"
	if _, err := sink.schemaValidationApplyCandidates(result); err == nil || !strings.Contains(err.Error(), "schema_evolution_disabled") {
		t.Fatalf("COLUMN_MISSING sem auto deveria bloquear: %v", err)
	}

	result.SourceSchema.Columns[0].Nullable = false
	sink.schemaEvolution = "auto"
	if _, err := sink.schemaValidationApplyCandidates(result); err == nil || !strings.Contains(err.Error(), "add_column_not_null") {
		t.Fatalf("ADD COLUMN NOT NULL deveria bloquear: %v", err)
	}

	result.SourceSchema.Columns[0].Nullable = true
	result.SourceSchema.Columns[0].TypeOID = 999999
	if _, err := sink.schemaValidationApplyCandidates(result); err == nil || !strings.Contains(err.Error(), "unsupported_type") {
		t.Fatalf("tipo desconhecido deveria bloquear: %v", err)
	}
}

func TestSchemaValidationRejectsNonAddColumnDifferences(t *testing.T) {
	sink := &postgresSink{schemaEvolution: "auto"}
	result := schemaValidationResult{
		Status:        compatibilityIncompatible,
		Schema:        "public",
		Table:         "clientes",
		LSN:           0x900,
		TransactionID: 7,
		Differences: []schemaDifference{{
			Type:   differenceTypeMismatch,
			Column: "email",
			Reason: "initial policy requires equal PostgreSQL type OID and typmod",
		}},
	}
	if _, err := sink.schemaValidationApplyCandidates(result); err == nil || !strings.Contains(err.Error(), string(differenceTypeMismatch)) {
		t.Fatalf("conflito de tipo deveria bloquear: %v", err)
	}
}

func createSchemaValidationTables(t *testing.T, source, destination *pgx.Conn, index int, sourceDDL, destinationDDL string) qualifiedTable {
	t.Helper()
	tableName := fmt.Sprintf("cdc_schema_validation_%s_%d", strconv.FormatInt(time.Now().UnixNano(), 36), index)
	table := qualifiedTable{schema: "public", name: tableName}
	tableSQL := pgx.Identifier{table.schema, table.name}.Sanitize()
	execSQL(t, source, "CREATE TABLE "+tableSQL+" "+sourceDDL)
	execSQL(t, destination, "CREATE TABLE "+tableSQL+" "+destinationDDL)
	t.Cleanup(func() {
		execSQL(t, source, "DROP TABLE IF EXISTS "+tableSQL)
		execSQL(t, destination, "DROP TABLE IF EXISTS "+tableSQL)
	})
	return table
}

func hasSchemaDifference(result schemaValidationResult, differenceType schemaDifferenceType, column string) bool {
	for _, difference := range result.Differences {
		if difference.Type != differenceType {
			continue
		}
		if column == "" || difference.Column == column {
			return true
		}
	}
	return false
}
