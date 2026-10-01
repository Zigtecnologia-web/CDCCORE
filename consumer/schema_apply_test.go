package main

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestReadTableSchemaTxReadsPrimaryKey(t *testing.T) {
	db := openDestinationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	table := createSchemaApplyTable(t, db, "id BIGSERIAL PRIMARY KEY, nome TEXT, email TEXT")
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("iniciar transacao: %v", err)
	}
	defer tx.Rollback(context.Background())

	schema, exists, err := readTableSchemaTx(ctx, tx, table)
	if err != nil {
		t.Fatalf("ler schema: %v", err)
	}
	if !exists {
		t.Fatalf("tabela %s deveria existir", table.key())
	}
	if !reflect.DeepEqual(schema.PrimaryKey, []string{"id"}) {
		t.Fatalf("PrimaryKey=%v, esperado [id]", schema.PrimaryKey)
	}
}

func TestReadTableSchemaTxReadsCompositePrimaryKeyInOrder(t *testing.T) {
	db := openDestinationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	table := createSchemaApplyTable(t, db, "tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, nome TEXT, PRIMARY KEY (tenant_id, id)")
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("iniciar transacao: %v", err)
	}
	defer tx.Rollback(context.Background())

	schema, exists, err := readTableSchemaTx(ctx, tx, table)
	if err != nil {
		t.Fatalf("ler schema: %v", err)
	}
	if !exists {
		t.Fatalf("tabela %s deveria existir", table.key())
	}
	if !reflect.DeepEqual(schema.PrimaryKey, []string{"tenant_id", "id"}) {
		t.Fatalf("PrimaryKey=%v, esperado [tenant_id id]", schema.PrimaryKey)
	}
}

func TestReadTableSchemaTxKeepsTableWithoutPrimaryKey(t *testing.T) {
	db := openDestinationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	table := createSchemaApplyTable(t, db, "id BIGINT, nome TEXT")
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("iniciar transacao: %v", err)
	}
	defer tx.Rollback(context.Background())

	schema, exists, err := readTableSchemaTx(ctx, tx, table)
	if err != nil {
		t.Fatalf("ler schema: %v", err)
	}
	if !exists {
		t.Fatalf("tabela %s deveria existir", table.key())
	}
	if len(schema.PrimaryKey) != 0 {
		t.Fatalf("PrimaryKey=%v, esperado vazio", schema.PrimaryKey)
	}
}

func TestPostgresSchemaApplyAddsNullableColumn(t *testing.T) {
	db := openDestinationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	table := createSchemaApplyTable(t, db, "id BIGINT PRIMARY KEY")
	sourceID := fmt.Sprintf("schema-apply-test-%d", time.Now().UnixNano())
	sink, err := newPostgresSinkWithOptions(ctx, cloneDestinationConfig(t, db), "schema-apply-test", postgresSinkOptions{
		applyMode:       "generic",
		schemaEvolution: "auto",
		includedTables:  []qualifiedTable{table},
	})
	if err != nil {
		t.Fatalf("criar sink: %v", err)
	}
	defer sink.Close(context.Background())

	tx := sourceTransaction{
		sourceID:      sourceID,
		commitLSN:     pglogrepl.LSN(0x901 + uint64(time.Now().UnixNano()%1000)),
		transactionID: 7,
		schemaChanges: []schemaChange{{
			LSN:            pglogrepl.LSN(0x900),
			TransactionID:  7,
			Schema:         table.schema,
			Table:          table.name,
			Classification: metadataColumnAdded,
			Column:         columnSchema{Name: "telefone", DataType: "text", TypeOID: pgtype.TextOID, Typmod: -1, Nullable: true},
			HasColumn:      true,
			Details:        map[string]any{"column": "telefone"},
		}},
		events: []event{{
			Type:    eventInsert,
			Schema:  table.schema,
			Table:   table.name,
			Columns: []string{"id", "telefone"},
			Data:    map[string]any{"id": int64(1), "telefone": "123"},
		}},
		metrics: newMetrics(),
	}
	if _, err := sink.ApplyTransaction(ctx, tx); err != nil {
		t.Fatalf("aplicar schema + dml: %v", err)
	}
	if !columnExists(t, db, table.schema, table.name, "telefone") {
		t.Fatal("coluna nullable nao foi aplicada no destination")
	}
	var telefone string
	if err := db.QueryRow(ctx, "SELECT telefone FROM "+pgx.Identifier{table.schema, table.name}.Sanitize()+" WHERE id = 1").Scan(&telefone); err != nil {
		t.Fatalf("consultar valor aplicado: %v", err)
	}
	if telefone != "123" {
		t.Fatalf("valor da coluna aplicada inesperado: %q", telefone)
	}
	if got := tx.metrics.schemaApplyAttempts.Load(); got != 1 {
		t.Fatalf("schema apply attempts=%d, esperado 1", got)
	}
	if got := tx.metrics.schemaApplySuccess.Load(); got != 1 {
		t.Fatalf("schema apply success=%d, esperado 1", got)
	}
	if got := tx.metrics.schemaApplyFailure.Load(); got != 0 {
		t.Fatalf("schema apply failure=%d, esperado 0", got)
	}
	if got := tx.metrics.schemaApplyRejected.Load(); got != 0 {
		t.Fatalf("schema apply rejected=%d, esperado 0", got)
	}
	if err := sink.Close(context.Background()); err != nil {
		t.Fatalf("simular restart fechando sink: %v", err)
	}
	sink = newSchemaApplySink(t, ctx, db, table, "auto")
	if _, err := sink.ApplyTransaction(ctx, tx); err != nil {
		t.Fatalf("replay da transacao aplicada: %v", err)
	}
	var rows int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{table.schema, table.name}.Sanitize()+" WHERE id = 1").Scan(&rows); err != nil {
		t.Fatalf("contar linhas apos replay: %v", err)
	}
	if rows != 1 {
		t.Fatalf("replay duplicou DML: linhas=%d", rows)
	}
}

func TestPostgresSchemaApplyRollsBackDDLWhenDMLFails(t *testing.T) {
	db := openDestinationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	table := createSchemaApplyTable(t, db, "id BIGINT PRIMARY KEY")
	sink := newSchemaApplySink(t, ctx, db, table, "auto")
	metrics := newMetrics()
	tx := sourceTransaction{
		sourceID:      fmt.Sprintf("schema-rollback-%d", time.Now().UnixNano()),
		commitLSN:     0x930,
		transactionID: 9,
		schemaChanges: []schemaChange{nullableColumnChange(table, "telefone", pgtype.TextOID, -1)},
		events: []event{{
			Type:    eventInsert,
			Schema:  table.schema,
			Table:   table.name,
			Columns: []string{"id", "telefone", "inexistente"},
			Data:    map[string]any{"id": int64(1), "telefone": "123", "inexistente": "falha"},
		}},
		metrics: metrics,
	}
	if _, err := sink.ApplyTransaction(ctx, tx); err == nil {
		t.Fatal("DML invalido deveria falhar depois do schema apply")
	}
	if columnExists(t, db, table.schema, table.name, "telefone") {
		t.Fatal("DDL nao foi revertido junto com o DML")
	}
	if transactionRecorded(t, db, tx.sourceID, tx.commitLSN) {
		t.Fatal("transacao com DML falho foi marcada como aplicada")
	}
	if got := metrics.schemaApplySuccess.Load(); got != 0 {
		t.Fatalf("schema apply revertido contou sucesso: %d", got)
	}
	if got := metrics.schemaApplyAttempts.Load(); got != 1 {
		t.Fatalf("schema apply revertido deveria contar tentativa: %d", got)
	}
	if got := metrics.schemaApplyFailure.Load(); got != 1 {
		t.Fatalf("schema apply revertido deveria contar falha: %d", got)
	}
	if got := metrics.schemaApplyRejected.Load(); got != 0 {
		t.Fatalf("schema apply revertido contou rejeicao: %d", got)
	}
	if err := sink.Close(context.Background()); err != nil {
		t.Fatalf("simular restart apos rollback: %v", err)
	}
	sink = newSchemaApplySink(t, ctx, db, table, "auto")
	tx.events = []event{{
		Type:    eventInsert,
		Schema:  table.schema,
		Table:   table.name,
		Columns: []string{"id", "telefone"},
		Data:    map[string]any{"id": int64(1), "telefone": "123"},
	}}
	if _, err := sink.ApplyTransaction(ctx, tx); err != nil {
		t.Fatalf("replay apos rollback: %v", err)
	}
	if !columnExists(t, db, table.schema, table.name, "telefone") {
		t.Fatal("replay nao reaplicou o schema apos rollback")
	}
	if !transactionRecorded(t, db, tx.sourceID, tx.commitLSN) {
		t.Fatal("replay valido nao foi marcado como aplicado")
	}
}

func TestPostgresSchemaApplyRecognizesCompatibleExistingColumn(t *testing.T) {
	db := openDestinationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	table := createSchemaApplyTable(t, db, "id BIGINT PRIMARY KEY, telefone TEXT")
	sink := newSchemaApplySink(t, ctx, db, table, "auto")
	metrics := newMetrics()
	tx := sourceTransaction{
		sourceID:      fmt.Sprintf("schema-existing-%d", time.Now().UnixNano()),
		commitLSN:     0x940,
		transactionID: 10,
		schemaChanges: []schemaChange{nullableColumnChange(table, "telefone", pgtype.TextOID, -1)},
		metrics:       metrics,
	}
	if _, err := sink.ApplyTransaction(ctx, tx); err != nil {
		t.Fatalf("coluna compativel deveria ser idempotente: %v", err)
	}
	if got := metrics.schemaApplySuccess.Load(); got != 1 {
		t.Fatalf("schema apply success=%d, esperado 1", got)
	}
}

func TestPostgresSchemaApplyRejectsIncompatibleExistingColumn(t *testing.T) {
	db := openDestinationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	table := createSchemaApplyTable(t, db, "id BIGINT PRIMARY KEY, telefone VARCHAR(5)")
	sink := newSchemaApplySink(t, ctx, db, table, "auto")
	metrics := newMetrics()
	tx := sourceTransaction{
		sourceID:      fmt.Sprintf("schema-conflict-%d", time.Now().UnixNano()),
		commitLSN:     0x950,
		transactionID: 11,
		schemaChanges: []schemaChange{nullableColumnChange(table, "telefone", pgtype.TextOID, -1)},
		metrics:       metrics,
	}
	if _, err := sink.ApplyTransaction(ctx, tx); err == nil || !strings.Contains(err.Error(), "schema conflict") {
		t.Fatalf("coluna incompativel deveria falhar: %v", err)
	}
	if got := metrics.schemaApplyFailure.Load(); got != 1 {
		t.Fatalf("schema apply failure=%d, esperado 1", got)
	}
	if transactionRecorded(t, db, tx.sourceID, tx.commitLSN) {
		t.Fatal("conflito de schema foi marcado como aplicado")
	}
}

func TestPostgresSchemaEvolutionModesDoNotApplyDDL(t *testing.T) {
	for _, mode := range []string{"disabled", "manual"} {
		t.Run(mode, func(t *testing.T) {
			db := openDestinationDatabase(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			table := createSchemaApplyTable(t, db, "id BIGINT PRIMARY KEY")
			sink := newSchemaApplySink(t, ctx, db, table, mode)
			metrics := newMetrics()
			_, err := sink.ApplyTransaction(ctx, sourceTransaction{
				sourceID:      fmt.Sprintf("schema-mode-%s-%d", mode, time.Now().UnixNano()),
				commitLSN:     0x960,
				transactionID: 12,
				schemaChanges: []schemaChange{nullableColumnChange(table, "telefone", pgtype.TextOID, -1)},
				metrics:       metrics,
			})
			if err != nil {
				t.Fatalf("modo %s sem DML: %v", mode, err)
			}
			if columnExists(t, db, table.schema, table.name, "telefone") {
				t.Fatalf("modo %s aplicou DDL", mode)
			}
			if got := metrics.schemaApplyRejected.Load(); got != 1 {
				t.Fatalf("schema apply rejected=%d, esperado 1", got)
			}
			if got := metrics.schemaApplyAttempts.Load(); got != 0 {
				t.Fatalf("schema apply rejected contou tentativa=%d, esperado 0", got)
			}
			if got := metrics.schemaApplySuccess.Load(); got != 0 {
				t.Fatalf("schema apply rejected contou sucesso=%d, esperado 0", got)
			}
			if got := metrics.schemaApplyFailure.Load(); got != 0 {
				t.Fatalf("schema apply rejected contou falha=%d, esperado 0", got)
			}
		})
	}
}

func TestSupportedPostgresTypeUsesOIDAndTypmodWhitelist(t *testing.T) {
	tests := []struct {
		column columnSchema
		want   string
		ok     bool
	}{
		{column: columnSchema{TypeOID: pgtype.TextOID, Typmod: -1}, want: "TEXT", ok: true},
		{column: columnSchema{TypeOID: pgtype.VarcharOID, Typmod: 34}, want: "CHARACTER VARYING(30)", ok: true},
		{column: columnSchema{TypeOID: pgtype.TimestampOID, Typmod: 3}, want: "TIMESTAMP WITHOUT TIME ZONE(3)", ok: true},
		{column: columnSchema{TypeOID: 999999, Typmod: -1, DataType: "text; DROP TABLE x"}, ok: false},
		{column: columnSchema{TypeOID: pgtype.TextOID, Typmod: 10}, ok: false},
	}
	for _, test := range tests {
		got, ok := supportedPostgresType(test.column)
		if ok != test.ok || got != test.want {
			t.Fatalf("supportedPostgresType(%+v)=(%q,%t), esperado (%q,%t)", test.column, got, ok, test.want, test.ok)
		}
	}
}

func TestValidPostgresIdentifier(t *testing.T) {
	for _, identifier := range []string{"public", "clientes", `nome\"com aspas`, "acao"} {
		if !validPostgresIdentifier(identifier) {
			t.Fatalf("identificador PostgreSQL valido rejeitado: %q", identifier)
		}
	}
	for _, identifier := range []string{"", "invalido\x00nome", string([]byte{0xff})} {
		if validPostgresIdentifier(identifier) {
			t.Fatalf("identificador invalido aceito: %q", identifier)
		}
	}
}

func newSchemaApplySink(t *testing.T, ctx context.Context, db *pgx.Conn, table qualifiedTable, mode string) *postgresSink {
	t.Helper()
	sink, err := newPostgresSinkWithOptions(ctx, cloneDestinationConfig(t, db), "schema-apply-test", postgresSinkOptions{
		applyMode:       "generic",
		schemaEvolution: mode,
		includedTables:  []qualifiedTable{table},
	})
	if err != nil {
		t.Fatalf("criar sink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close(context.Background()) })
	return sink
}

func nullableColumnChange(table qualifiedTable, name string, oid uint32, typmod int32) schemaChange {
	return schemaChange{
		LSN:            0x900,
		TransactionID:  7,
		Schema:         table.schema,
		Table:          table.name,
		Classification: metadataColumnAdded,
		Column:         columnSchema{Name: name, TypeOID: oid, Typmod: typmod, Nullable: true},
		HasColumn:      true,
		Details:        map[string]any{"column": name},
	}
}

func transactionRecorded(t *testing.T, db *pgx.Conn, sourceID string, lsn pglogrepl.LSN) bool {
	t.Helper()
	var exists bool
	if err := db.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM cdc_applied_transactions
			WHERE source_id = $1 AND commit_lsn = $2::pg_lsn
		)
	`, sourceID, lsn.String()).Scan(&exists); err != nil {
		t.Fatalf("consultar idempotencia: %v", err)
	}
	return exists
}

func TestPostgresSchemaApplyBlocksNotNullColumn(t *testing.T) {
	db := openDestinationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	table := createSchemaApplyTable(t, db, "id BIGINT PRIMARY KEY")
	sourceID := fmt.Sprintf("schema-apply-block-test-%d", time.Now().UnixNano())
	sink, err := newPostgresSinkWithOptions(ctx, cloneDestinationConfig(t, db), "schema-apply-block-test", postgresSinkOptions{
		applyMode:       "generic",
		schemaEvolution: "auto",
		includedTables:  []qualifiedTable{table},
	})
	if err != nil {
		t.Fatalf("criar sink: %v", err)
	}
	defer sink.Close(context.Background())

	_, err = sink.ApplyTransaction(ctx, sourceTransaction{
		sourceID:      sourceID,
		commitLSN:     pglogrepl.LSN(0x911 + uint64(time.Now().UnixNano()%1000)),
		transactionID: 8,
		schemaChanges: []schemaChange{{
			LSN:            pglogrepl.LSN(0x910),
			TransactionID:  8,
			Schema:         table.schema,
			Table:          table.name,
			Classification: metadataColumnAdded,
			Column:         columnSchema{Name: "codigo", DataType: "text", TypeOID: pgtype.TextOID, Typmod: -1, Nullable: false},
			HasColumn:      true,
			Details:        map[string]any{"column": "codigo"},
		}},
		events: []event{{
			Type:    eventInsert,
			Schema:  table.schema,
			Table:   table.name,
			Columns: []string{"id", "codigo"},
			Data:    map[string]any{"id": int64(1), "codigo": "A"},
		}},
		metrics: newMetrics(),
	})
	if err == nil || !strings.Contains(err.Error(), string(differenceColumnMissing)) {
		t.Fatalf("ADD COLUMN NOT NULL deveria bloquear apply e falhar no DML, erro=%v", err)
	}
	if columnExists(t, db, table.schema, table.name, "codigo") {
		t.Fatal("coluna NOT NULL foi aplicada automaticamente")
	}
}

func createSchemaApplyTable(t *testing.T, db *pgx.Conn, definition string) qualifiedTable {
	t.Helper()
	table := qualifiedTable{
		schema: "public",
		name:   fmt.Sprintf("cdc_schema_apply_%d", time.Now().UnixNano()),
	}
	tableSQL := pgx.Identifier{table.schema, table.name}.Sanitize()
	execSQL(t, db, "CREATE TABLE "+tableSQL+" ("+definition+")")
	t.Cleanup(func() {
		execSQL(t, db, "DROP TABLE IF EXISTS "+tableSQL)
	})
	return table
}
