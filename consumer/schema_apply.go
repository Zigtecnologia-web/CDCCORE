package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type schemaApplyResult string

const (
	schemaApplyStarted        schemaApplyResult = "started"
	schemaApplyStaged         schemaApplyResult = "staged"
	schemaApplyApplied        schemaApplyResult = "success"
	schemaApplyAlreadyApplied schemaApplyResult = "already_applied"
	schemaApplyBlocked        schemaApplyResult = "rejected"
	schemaApplyFailed         schemaApplyResult = "failure"
)

type schemaApplyRisk string

const (
	schemaApplyRiskLow    schemaApplyRisk = "LOW"
	schemaApplyRiskMedium schemaApplyRisk = "MEDIUM"
	schemaApplyRiskHigh   schemaApplyRisk = "HIGH"
)

func (sink *postgresSink) applySchemaChanges(ctx context.Context, tx pgx.Tx, sourceTx sourceTransaction) ([]schemaChange, error) {
	applied := make([]schemaChange, 0)
	for _, change := range sourceTx.schemaChanges {
		if !sink.shouldConsiderSchemaChange(change) {
			sink.logSchemaApply(sourceTx, change, schemaApplyBlocked, schemaApplyRiskHigh, "table_not_supported")
			continue
		}
		if sink.schemaEvolution != "auto" {
			reason := "schema_evolution_" + sink.schemaEvolution
			sink.logSchemaApply(sourceTx, change, schemaApplyBlocked, schemaApplyRiskHigh, reason)
			continue
		}
		succeeded, err := sink.applyAutomaticSchemaChange(ctx, tx, sourceTx, change)
		if err != nil {
			return applied, err
		}
		if succeeded {
			applied = append(applied, change)
		}
	}
	return applied, nil
}

func (sink *postgresSink) applyAutomaticSchemaChange(ctx context.Context, tx pgx.Tx, sourceTx sourceTransaction, change schemaChange) (bool, error) {
	if change.Classification != metadataColumnAdded {
		sink.logSchemaApply(sourceTx, change, schemaApplyBlocked, schemaApplyRiskHigh, "operation_not_allowed")
		return false, nil
	}
	if !change.HasColumn {
		sink.logSchemaApply(sourceTx, change, schemaApplyBlocked, schemaApplyRiskMedium, "source_column_metadata_missing")
		return false, nil
	}
	if !change.Column.Nullable {
		sink.logSchemaApply(sourceTx, change, schemaApplyBlocked, schemaApplyRiskMedium, "add_column_not_null")
		return false, nil
	}
	if !validPostgresIdentifier(change.Schema) || !validPostgresIdentifier(change.Table) || !validPostgresIdentifier(change.Column.Name) {
		sink.logSchemaApply(sourceTx, change, schemaApplyBlocked, schemaApplyRiskMedium, "invalid_identifier")
		return false, nil
	}
	typeSQL, ok := supportedPostgresType(change.Column)
	if !ok {
		sink.logSchemaApply(sourceTx, change, schemaApplyBlocked, schemaApplyRiskMedium, "unsupported_type")
		return false, nil
	}

	table := qualifiedTable{schema: change.Schema, name: change.Table}
	destination, exists, err := readTableSchemaTx(ctx, tx, table)
	if err != nil {
		sink.logSchemaApply(sourceTx, change, schemaApplyFailed, schemaApplyRiskLow, "read_destination_failed")
		return false, err
	}
	if !exists {
		sink.logSchemaApply(sourceTx, change, schemaApplyBlocked, schemaApplyRiskMedium, "destination_table_missing")
		return false, nil
	}
	if existing, exists := columnsByName(destination.Columns)[change.Column.Name]; exists {
		if columnsCompatible(change.Column, existing) {
			delete(sink.metadataCache, table.key())
			sink.logSchemaApply(sourceTx, change, schemaApplyAlreadyApplied, schemaApplyRiskLow, "column_already_compatible")
			return true, nil
		}
		sink.logSchemaApply(sourceTx, change, schemaApplyFailed, schemaApplyRiskMedium, "schema_conflict")
		return false, fmt.Errorf("schema conflict em %s.%s: coluna %s existe com definicao incompativel", change.Schema, change.Table, change.Column.Name)
	}

	sink.logSchemaApply(sourceTx, change, schemaApplyStarted, schemaApplyRiskLow, "add_nullable_column")
	query := fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN %s %s",
		pgx.Identifier{change.Schema, change.Table}.Sanitize(),
		pgx.Identifier{change.Column.Name}.Sanitize(),
		typeSQL,
	)
	if _, err := tx.Exec(ctx, query); err != nil {
		sink.logSchemaApply(sourceTx, change, schemaApplyFailed, schemaApplyRiskLow, "ddl_failed")
		return false, fmt.Errorf("aplicar mudanca de metadata %s em %s.%s: %w", change.Classification, change.Schema, change.Table, err)
	}
	after, exists, err := readTableSchemaTx(ctx, tx, table)
	if err != nil {
		sink.logSchemaApply(sourceTx, change, schemaApplyFailed, schemaApplyRiskLow, "revalidation_failed")
		return false, err
	}
	if !exists {
		sink.logSchemaApply(sourceTx, change, schemaApplyFailed, schemaApplyRiskLow, "destination_table_missing_after_apply")
		return false, fmt.Errorf("tabela %s desapareceu apos schema apply", table.key())
	}
	applied, exists := columnsByName(after.Columns)[change.Column.Name]
	if !exists || !columnsCompatible(change.Column, applied) {
		sink.logSchemaApply(sourceTx, change, schemaApplyFailed, schemaApplyRiskLow, "revalidation_incompatible")
		return false, fmt.Errorf("schema apply %s em %s.%s nao revalidou como compativel", change.Classification, change.Schema, change.Table)
	}

	delete(sink.metadataCache, table.key())
	sink.logSchemaApply(sourceTx, change, schemaApplyStaged, schemaApplyRiskLow, "awaiting_destination_commit")
	return true, nil
}

func (sink *postgresSink) shouldConsiderSchemaChange(change schemaChange) bool {
	table := qualifiedTable{schema: change.Schema, name: change.Table}
	if sink.applyMode == "generic" {
		_, included := sink.includedTables[table.key()]
		return included
	}
	return change.Schema == "public" && (change.Table == "clientes" || change.Table == "enderecos")
}

func supportedPostgresType(column columnSchema) (string, bool) {
	switch column.TypeOID {
	case pgtype.TextOID:
		return fixedPostgresType("TEXT", column.Typmod)
	case pgtype.Int4OID:
		return fixedPostgresType("INTEGER", column.Typmod)
	case pgtype.Int8OID:
		return fixedPostgresType("BIGINT", column.Typmod)
	case pgtype.BoolOID:
		return fixedPostgresType("BOOLEAN", column.Typmod)
	case pgtype.JSONBOID:
		return fixedPostgresType("JSONB", column.Typmod)
	case pgtype.DateOID:
		return fixedPostgresType("DATE", column.Typmod)
	case pgtype.UUIDOID:
		return fixedPostgresType("UUID", column.Typmod)
	case pgtype.VarcharOID:
		if column.Typmod == -1 {
			return "CHARACTER VARYING", true
		}
		if column.Typmod >= 4 {
			return fmt.Sprintf("CHARACTER VARYING(%d)", column.Typmod-4), true
		}
	case pgtype.NumericOID:
		if column.Typmod == -1 {
			return "NUMERIC", true
		}
		if column.Typmod >= 4 {
			typmod := column.Typmod - 4
			precision := (typmod >> 16) & 0xffff
			scale := int16(typmod & 0xffff)
			if precision > 0 {
				return fmt.Sprintf("NUMERIC(%d,%d)", precision, scale), true
			}
		}
	case pgtype.TimestampOID:
		return temporalPostgresType("TIMESTAMP WITHOUT TIME ZONE", column.Typmod)
	case pgtype.TimestamptzOID:
		return temporalPostgresType("TIMESTAMP WITH TIME ZONE", column.Typmod)
	case pgtype.TimeOID:
		return temporalPostgresType("TIME WITHOUT TIME ZONE", column.Typmod)
	case pgtype.TimetzOID:
		return temporalPostgresType("TIME WITH TIME ZONE", column.Typmod)
	}
	return "", false
}

func fixedPostgresType(name string, typmod int32) (string, bool) {
	if typmod != -1 {
		return "", false
	}
	return name, true
}

func temporalPostgresType(name string, typmod int32) (string, bool) {
	if typmod == -1 {
		return name, true
	}
	if typmod >= 0 && typmod <= 6 {
		return fmt.Sprintf("%s(%d)", name, typmod), true
	}
	return "", false
}

func validPostgresIdentifier(identifier string) bool {
	return identifier != "" && utf8.ValidString(identifier) && !strings.ContainsRune(identifier, '\x00')
}

func columnsCompatible(source, destination columnSchema) bool {
	if source.TypeOID != destination.TypeOID || source.Typmod != destination.Typmod {
		return false
	}
	if source.Nullable && !destination.Nullable {
		return false
	}
	return true
}

func readTableSchemaTx(ctx context.Context, tx pgx.Tx, table qualifiedTable) (tableSchema, bool, error) {
	var relationOID uint32
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT relation.oid::oid::int, true
		FROM pg_catalog.pg_class AS relation
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = $1
		  AND relation.relname = $2
		  AND relation.relkind IN ('r', 'p')
	`, table.schema, table.name).Scan(&relationOID, &exists); err != nil {
		if err == pgx.ErrNoRows {
			return tableSchema{Schema: table.schema, Table: table.name}, false, nil
		}
		return tableSchema{}, false, fmt.Errorf("consultar tabela %s no destination: %w", table.key(), err)
	}

	rows, err := tx.Query(ctx, `
		SELECT
			attribute.attname,
			pg_catalog.format_type(attribute.atttypid, attribute.atttypmod),
			attribute.atttypid::oid::int,
			attribute.atttypmod,
			NOT attribute.attnotnull,
			attribute.atthasdef
		FROM pg_catalog.pg_attribute AS attribute
		WHERE attribute.attrelid = $1::oid
		  AND attribute.attnum > 0
		  AND NOT attribute.attisdropped
		ORDER BY attribute.attnum
	`, relationOID)
	if err != nil {
		return tableSchema{}, false, fmt.Errorf("consultar colunas de %s no destination: %w", table.key(), err)
	}
	defer rows.Close()

	schema := tableSchema{Schema: table.schema, Table: table.name}
	for rows.Next() {
		var column columnSchema
		if err := rows.Scan(&column.Name, &column.DataType, &column.TypeOID, &column.Typmod, &column.Nullable, &column.HasDefault); err != nil {
			return tableSchema{}, false, fmt.Errorf("ler coluna de %s no destination: %w", table.key(), err)
		}
		schema.Columns = append(schema.Columns, column)
	}
	if err := rows.Err(); err != nil {
		return tableSchema{}, false, fmt.Errorf("listar colunas de %s no destination: %w", table.key(), err)
	}

	pkRows, err := tx.Query(ctx, `
		SELECT attribute.attname
		FROM pg_catalog.pg_index AS index
		JOIN LATERAL unnest(index.indkey) WITH ORDINALITY AS key(attnum, position) ON true
		JOIN pg_catalog.pg_attribute AS attribute
		  ON attribute.attrelid = index.indrelid
		 AND attribute.attnum = key.attnum
		WHERE index.indrelid = $1::oid
		  AND index.indisprimary
		ORDER BY key.position
	`, relationOID)
	if err != nil {
		return tableSchema{}, false, fmt.Errorf("consultar chave primaria de %s no destination: %w", table.key(), err)
	}
	defer pkRows.Close()
	for pkRows.Next() {
		var column string
		if err := pkRows.Scan(&column); err != nil {
			return tableSchema{}, false, fmt.Errorf("ler chave primaria de %s no destination: %w", table.key(), err)
		}
		schema.PrimaryKey = append(schema.PrimaryKey, column)
	}
	if err := pkRows.Err(); err != nil {
		return tableSchema{}, false, fmt.Errorf("listar chave primaria de %s no destination: %w", table.key(), err)
	}
	return schema, true, nil
}

func (sink *postgresSink) logSchemaApply(sourceTx sourceTransaction, change schemaChange, result schemaApplyResult, risk schemaApplyRisk, reason string) {
	recordSchemaApplyMetric(sourceTx.metrics, result)

	column := change.Column.Name
	if column == "" {
		column, _ = change.Details["column"].(string)
	}
	level := "info"
	if result == schemaApplyFailed {
		level = "error"
	} else if result == schemaApplyBlocked {
		level = "warn"
	}
	operation := "NONE"
	if change.Classification == metadataColumnAdded {
		operation = "ADD_COLUMN"
	}
	log.Printf(
		"level=%s event=schema_apply source_id=%s transaction_id=%d lsn=%s schema=%s table=%s operation=%s classification=%s column=%s mode=%s status=%s risk=%s reason=%s",
		level,
		sourceTx.sourceID,
		change.TransactionID,
		change.LSN,
		change.Schema,
		change.Table,
		operation,
		change.Classification,
		column,
		sink.schemaEvolution,
		result,
		risk,
		reason,
	)
}

func recordSchemaApplyMetric(metrics *metrics, result schemaApplyResult) {
	if metrics == nil {
		return
	}
	switch result {
	case schemaApplyStarted:
		metrics.schemaApplyAttempts.Add(1)
	case schemaApplyApplied:
		metrics.schemaApplySuccess.Add(1)
	case schemaApplyFailed:
		metrics.schemaApplyFailure.Add(1)
	case schemaApplyBlocked:
		metrics.schemaApplyRejected.Add(1)
	}
}
