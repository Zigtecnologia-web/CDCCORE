package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

type compatibilityStatus string

const (
	compatibilityCompatible   compatibilityStatus = "COMPATIBLE"
	compatibilityIncompatible compatibilityStatus = "INCOMPATIBLE"
	compatibilityUnknown      compatibilityStatus = "UNKNOWN"
)

type schemaDifferenceType string

const (
	differenceTableMissing        schemaDifferenceType = "TABLE_MISSING"
	differenceColumnMissing       schemaDifferenceType = "COLUMN_MISSING"
	differenceColumnExtra         schemaDifferenceType = "COLUMN_EXTRA"
	differenceTypeMismatch        schemaDifferenceType = "TYPE_MISMATCH"
	differenceNullabilityMismatch schemaDifferenceType = "NULLABILITY_MISMATCH"
	differencePrimaryKeyMismatch  schemaDifferenceType = "PRIMARY_KEY_MISMATCH"
)

type tableSchema struct {
	Schema     string
	Table      string
	Columns    []columnSchema
	PrimaryKey []string
}

type columnSchema struct {
	Name       string
	DataType   string
	TypeOID    uint32
	Typmod     int32
	Nullable   bool
	HasDefault bool
}

type schemaValidationResult struct {
	Status            compatibilityStatus
	Schema            string
	Table             string
	LSN               pglogrepl.LSN
	TransactionID     uint32
	SourceSchema      tableSchema
	DestinationSchema tableSchema
	Differences       []schemaDifference
}

type schemaDifference struct {
	Type             schemaDifferenceType
	Column           string
	SourceValue      string
	DestinationValue string
	Reason           string
}

type schemaValidator interface {
	ShouldValidate(table qualifiedTable) bool
	Validate(ctx context.Context, table qualifiedTable, lsn pglogrepl.LSN, xid uint32) schemaValidationResult
	Close(ctx context.Context) error
}

type postgresSchemaValidator struct {
	source         *schemaCatalogReader
	destination    *schemaCatalogReader
	includedTables map[string]struct{}
}

type schemaCatalogReader struct {
	name string
	conn *pgx.Conn
}

func newPostgresSchemaValidator(
	ctx context.Context,
	sourceConfig *pgx.ConnConfig,
	destinationConfig *pgx.ConnConfig,
	includedTables []qualifiedTable,
) (*postgresSchemaValidator, error) {
	sourceConn, err := pgx.ConnectConfig(ctx, sourceConfig)
	if err != nil {
		return nil, fmt.Errorf("conexao source para validacao de schema: %w", err)
	}
	destinationConn, err := pgx.ConnectConfig(ctx, destinationConfig)
	if err != nil {
		_ = sourceConn.Close(context.Background())
		return nil, fmt.Errorf("conexao destination para validacao de schema: %w", err)
	}

	included := make(map[string]struct{}, len(includedTables))
	for _, table := range includedTables {
		included[table.key()] = struct{}{}
	}
	return &postgresSchemaValidator{
		source:         &schemaCatalogReader{name: "source", conn: sourceConn},
		destination:    &schemaCatalogReader{name: "destination", conn: destinationConn},
		includedTables: included,
	}, nil
}

func (validator *postgresSchemaValidator) Close(ctx context.Context) error {
	var err error
	if validator.source != nil && validator.source.conn != nil {
		err = validator.source.conn.Close(ctx)
	}
	if validator.destination != nil && validator.destination.conn != nil {
		if closeErr := validator.destination.conn.Close(ctx); err == nil {
			err = closeErr
		}
	}
	return err
}

func (validator *postgresSchemaValidator) ShouldValidate(table qualifiedTable) bool {
	if len(validator.includedTables) == 0 {
		return true
	}
	_, included := validator.includedTables[table.key()]
	return included
}

func (validator *postgresSchemaValidator) Validate(ctx context.Context, table qualifiedTable, lsn pglogrepl.LSN, xid uint32) schemaValidationResult {
	result := schemaValidationResult{
		Status:        compatibilityUnknown,
		Schema:        table.schema,
		Table:         table.name,
		LSN:           lsn,
		TransactionID: xid,
	}

	sourceSchema, sourceExists, err := validator.source.ReadTableSchema(ctx, table)
	if err != nil {
		result.Differences = append(result.Differences, schemaDifference{
			Reason: fmt.Sprintf("read_source_schema_failed: %v", err),
		})
		return result
	}
	if !sourceExists {
		result.Differences = append(result.Differences, schemaDifference{
			Type:   differenceTableMissing,
			Reason: "source table not found",
		})
		return result
	}
	destinationSchema, destinationExists, err := validator.destination.ReadTableSchema(ctx, table)
	if err != nil {
		result.Differences = append(result.Differences, schemaDifference{
			Reason: fmt.Sprintf("read_destination_schema_failed: %v", err),
		})
		return result
	}
	if !destinationExists {
		result.Status = compatibilityIncompatible
		result.SourceSchema = sourceSchema
		result.Differences = append(result.Differences, schemaDifference{
			Type:   differenceTableMissing,
			Reason: "destination table not found",
		})
		return result
	}

	return compareTableSchemas(sourceSchema, destinationSchema, lsn, xid)
}

func (reader *schemaCatalogReader) ReadTableSchema(ctx context.Context, table qualifiedTable) (tableSchema, bool, error) {
	var relationOID uint32
	var exists bool
	if err := reader.conn.QueryRow(ctx, `
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
		return tableSchema{}, false, fmt.Errorf("consultar tabela %s no %s: %w", table.key(), reader.name, err)
	}

	rows, err := reader.conn.Query(ctx, `
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
		return tableSchema{}, false, fmt.Errorf("consultar colunas de %s no %s: %w", table.key(), reader.name, err)
	}
	defer rows.Close()

	schema := tableSchema{Schema: table.schema, Table: table.name}
	for rows.Next() {
		var column columnSchema
		if err := rows.Scan(&column.Name, &column.DataType, &column.TypeOID, &column.Typmod, &column.Nullable, &column.HasDefault); err != nil {
			return tableSchema{}, false, fmt.Errorf("ler coluna de %s no %s: %w", table.key(), reader.name, err)
		}
		schema.Columns = append(schema.Columns, column)
	}
	if err := rows.Err(); err != nil {
		return tableSchema{}, false, fmt.Errorf("listar colunas de %s no %s: %w", table.key(), reader.name, err)
	}

	pkRows, err := reader.conn.Query(ctx, `
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
		return tableSchema{}, false, fmt.Errorf("consultar chave primaria de %s no %s: %w", table.key(), reader.name, err)
	}
	defer pkRows.Close()
	for pkRows.Next() {
		var column string
		if err := pkRows.Scan(&column); err != nil {
			return tableSchema{}, false, fmt.Errorf("ler chave primaria de %s no %s: %w", table.key(), reader.name, err)
		}
		schema.PrimaryKey = append(schema.PrimaryKey, column)
	}
	if err := pkRows.Err(); err != nil {
		return tableSchema{}, false, fmt.Errorf("listar chave primaria de %s no %s: %w", table.key(), reader.name, err)
	}

	return schema, true, nil
}

func compareTableSchemas(source, destination tableSchema, lsn pglogrepl.LSN, xid uint32) schemaValidationResult {
	// Initial compatibility policy for the current named-column DML:
	// source columns must exist in destination; PostgreSQL type OID and typmod
	// must match exactly; destination may be more nullable, but not stricter;
	// extra destination columns are allowed only when nullable or defaulted; and
	// primary key columns must match in order.
	result := schemaValidationResult{
		Status:            compatibilityCompatible,
		Schema:            source.Schema,
		Table:             source.Table,
		LSN:               lsn,
		TransactionID:     xid,
		SourceSchema:      source,
		DestinationSchema: destination,
	}

	sourceByName := columnsByName(source.Columns)
	destinationByName := columnsByName(destination.Columns)
	for _, sourceColumn := range source.Columns {
		destinationColumn, exists := destinationByName[sourceColumn.Name]
		if !exists {
			result.Differences = append(result.Differences, schemaDifference{
				Type:        differenceColumnMissing,
				Column:      sourceColumn.Name,
				SourceValue: columnDescription(sourceColumn),
				Reason:      "source column is absent in destination",
			})
			continue
		}
		if sourceColumn.TypeOID != destinationColumn.TypeOID || sourceColumn.Typmod != destinationColumn.Typmod {
			result.Differences = append(result.Differences, schemaDifference{
				Type:             differenceTypeMismatch,
				Column:           sourceColumn.Name,
				SourceValue:      columnTypeDescription(sourceColumn),
				DestinationValue: columnTypeDescription(destinationColumn),
				Reason:           "initial policy requires equal PostgreSQL type OID and typmod",
			})
		}
		if sourceColumn.Nullable && !destinationColumn.Nullable {
			result.Differences = append(result.Differences, schemaDifference{
				Type:             differenceNullabilityMismatch,
				Column:           sourceColumn.Name,
				SourceValue:      "NULLABLE",
				DestinationValue: "NOT NULL",
				Reason:           "source can produce NULL values that destination rejects",
			})
		}
	}

	for _, destinationColumn := range destination.Columns {
		if _, exists := sourceByName[destinationColumn.Name]; exists {
			continue
		}
		if !destinationColumn.Nullable && !destinationColumn.HasDefault {
			result.Differences = append(result.Differences, schemaDifference{
				Type:             differenceColumnExtra,
				Column:           destinationColumn.Name,
				DestinationValue: columnDescription(destinationColumn),
				Reason:           "destination extra NOT NULL column without DEFAULT can reject INSERT events",
			})
		}
	}

	if !sameStringSlice(source.PrimaryKey, destination.PrimaryKey) {
		result.Differences = append(result.Differences, schemaDifference{
			Type:             differencePrimaryKeyMismatch,
			SourceValue:      strings.Join(source.PrimaryKey, ","),
			DestinationValue: strings.Join(destination.PrimaryKey, ","),
			Reason:           "source and destination primary keys differ",
		})
	}

	if len(result.Differences) > 0 {
		result.Status = compatibilityIncompatible
	}
	return result
}

func (sink *postgresSink) validateTransactionSchema(ctx context.Context, tx pgx.Tx, sourceTx sourceTransaction) ([]schemaChange, error) {
	changes := make([]schemaChange, 0)
	for _, result := range sourceTx.schemaValidations {
		logSchemaValidationResult(sourceTx.sourceID, result, sourceTx.metrics)
		switch result.Status {
		case compatibilityCompatible:
			continue
		case compatibilityUnknown:
			return nil, fmt.Errorf("schema validation rejected %s.%s: UNKNOWN", result.Schema, result.Table)
		case compatibilityIncompatible:
			candidates, err := sink.schemaValidationApplyCandidates(result)
			if err != nil {
				return nil, err
			}
			changes = append(changes, candidates...)
		default:
			return nil, fmt.Errorf("schema validation rejected %s.%s: status desconhecido %q", result.Schema, result.Table, result.Status)
		}
	}
	return changes, nil
}

func (sink *postgresSink) schemaValidationApplyCandidates(result schemaValidationResult) ([]schemaChange, error) {
	candidates := make([]schemaChange, 0)
	sourceColumns := columnsByName(result.SourceSchema.Columns)
	destinationColumns := columnsByName(result.DestinationSchema.Columns)
	for _, difference := range result.Differences {
		if difference.Type != differenceColumnMissing {
			return nil, fmt.Errorf(
				"schema validation rejected %s.%s: %s column=%s reason=%s",
				result.Schema,
				result.Table,
				difference.Type,
				difference.Column,
				difference.Reason,
			)
		}
		if sink.schemaEvolution != "auto" {
			return nil, fmt.Errorf("schema validation rejected %s.%s: COLUMN_MISSING column=%s schema_evolution_%s", result.Schema, result.Table, difference.Column, sink.schemaEvolution)
		}
		if sink.applyMode != "generic" {
			return nil, fmt.Errorf("schema validation rejected %s.%s: COLUMN_MISSING column=%s apply_mode_%s", result.Schema, result.Table, difference.Column, sink.applyMode)
		}
		column, exists := sourceColumns[difference.Column]
		if !exists {
			return nil, fmt.Errorf("schema validation rejected %s.%s: COLUMN_MISSING column=%s source_metadata_missing", result.Schema, result.Table, difference.Column)
		}
		if _, exists := destinationColumns[difference.Column]; exists {
			return nil, fmt.Errorf("schema validation rejected %s.%s: COLUMN_MISSING column=%s destination_conflict", result.Schema, result.Table, difference.Column)
		}
		if !column.Nullable {
			return nil, fmt.Errorf("schema validation rejected %s.%s: COLUMN_MISSING column=%s add_column_not_null", result.Schema, result.Table, difference.Column)
		}
		if _, ok := supportedPostgresType(column); !ok {
			return nil, fmt.Errorf("schema validation rejected %s.%s: COLUMN_MISSING column=%s unsupported_type", result.Schema, result.Table, difference.Column)
		}
		candidates = append(candidates, schemaChange{
			LSN:            result.LSN,
			TransactionID:  result.TransactionID,
			Schema:         result.Schema,
			Table:          result.Table,
			Classification: metadataColumnAdded,
			Column:         column,
			HasColumn:      true,
			Details: map[string]any{
				"column": column.Name,
				"basis":  "schema_validation_column_missing",
			},
		})
	}
	return candidates, nil
}

func (sink *postgresSink) validateDestinationForEvents(ctx context.Context, tx pgx.Tx, sourceTx sourceTransaction) error {
	for _, event := range sourceTx.events {
		table := qualifiedTable{schema: event.Schema, name: event.Table}
		destination, exists, err := readTableSchemaTx(ctx, tx, table)
		if err != nil {
			return err
		}
		result := schemaValidationResult{
			Status:            compatibilityCompatible,
			Schema:            event.Schema,
			Table:             event.Table,
			TransactionID:     sourceTx.transactionID,
			DestinationSchema: destination,
		}
		if event.LSN != "" {
			if lsn, err := pglogrepl.ParseLSN(event.LSN); err == nil {
				result.LSN = lsn
			}
		}
		if !exists {
			result.Status = compatibilityIncompatible
			result.Differences = append(result.Differences, schemaDifference{
				Type:   differenceTableMissing,
				Reason: "destination table not found",
			})
			logSchemaValidationResult(sourceTx.sourceID, result, sourceTx.metrics)
			return fmt.Errorf("schema validation rejected %s.%s: TABLE_MISSING", event.Schema, event.Table)
		}
		destinationColumns := columnsByName(destination.Columns)
		for _, column := range eventRequiredColumns(event) {
			if _, exists := destinationColumns[column]; !exists {
				result.Status = compatibilityIncompatible
				result.Differences = append(result.Differences, schemaDifference{
					Type:   differenceColumnMissing,
					Column: column,
					Reason: "DML column is absent in destination",
				})
			}
		}
		if sink.applyMode == "generic" && len(destination.PrimaryKey) == 0 {
			result.Status = compatibilityIncompatible
			result.Differences = append(result.Differences, schemaDifference{
				Type:   differencePrimaryKeyMismatch,
				Reason: "destination table has no primary key for generic CDC",
			})
		}
		logSchemaValidationResult(sourceTx.sourceID, result, sourceTx.metrics)
		if result.Status != compatibilityCompatible {
			first := result.Differences[0]
			return fmt.Errorf("schema validation rejected %s.%s: %s column=%s reason=%s", event.Schema, event.Table, first.Type, first.Column, first.Reason)
		}
	}
	return nil
}

func eventRequiredColumns(event event) []string {
	seen := make(map[string]struct{})
	columns := make([]string, 0, len(event.Data)+len(event.OldData))
	for _, values := range []map[string]any{event.Data, event.OldData} {
		for column := range values {
			if _, exists := seen[column]; exists {
				continue
			}
			seen[column] = struct{}{}
			columns = append(columns, column)
		}
	}
	return columns
}

func columnsByName(columns []columnSchema) map[string]columnSchema {
	result := make(map[string]columnSchema, len(columns))
	for _, column := range columns {
		result[column.Name] = column
	}
	return result
}

func sameStringSlice(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func columnDescription(column columnSchema) string {
	nullability := "NULL"
	if !column.Nullable {
		nullability = "NOT NULL"
	}
	defaultValue := "NO DEFAULT"
	if column.HasDefault {
		defaultValue = "DEFAULT"
	}
	return fmt.Sprintf("%s %s %s", columnTypeDescription(column), nullability, defaultValue)
}

func columnTypeDescription(column columnSchema) string {
	return fmt.Sprintf("%s oid=%d typmod=%d", column.DataType, column.TypeOID, column.Typmod)
}

func logSchemaValidationResult(sourceID string, result schemaValidationResult, metrics *metrics) {
	if metrics != nil {
		metrics.schemaValidationTotal.Add(string(result.Status))
	}
	if len(result.Differences) == 0 {
		log.Printf(
			"level=info event=schema_validation source_id=%s transaction_id=%d lsn=%s schema=%s table=%s validation_result=%s reason=compatible",
			sourceID,
			result.TransactionID,
			result.LSN,
			result.Schema,
			result.Table,
			result.Status,
		)
		return
	}

	for _, difference := range result.Differences {
		details, err := json.Marshal(map[string]string{
			"source_value":      difference.SourceValue,
			"destination_value": difference.DestinationValue,
			"reason":            difference.Reason,
		})
		if err != nil {
			details = []byte(`{"serialization_error":true}`)
		}
		log.Printf(
			"level=warn event=schema_validation source_id=%s transaction_id=%d lsn=%s schema=%s table=%s validation_result=%s reason=%s difference=%s column=%s details=%s",
			sourceID,
			result.TransactionID,
			result.LSN,
			result.Schema,
			result.Table,
			result.Status,
			difference.Reason,
			difference.Type,
			difference.Column,
			details,
		)
	}
}
