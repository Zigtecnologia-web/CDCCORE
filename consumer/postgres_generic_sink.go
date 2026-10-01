package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

type destinationTableMetadata struct {
	columns         []string
	columnSet       map[string]struct{}
	primaryKey      []string
	sourceSignature string
}

func (sink *postgresSink) applyGenericEvent(ctx context.Context, tx pgx.Tx, event event) error {
	table := qualifiedTable{schema: event.Schema, name: event.Table}
	if _, allowed := sink.includedTables[table.key()]; !allowed {
		return fmt.Errorf("tabela %s nao esta em CDC_TABLE_INCLUDE", table.key())
	}

	metadata, err := sink.destinationMetadata(ctx, tx, table, event.Columns)
	if err != nil {
		return err
	}
	if err := validateEventColumns(table, metadata, event); err != nil {
		return err
	}

	switch event.Type {
	case eventInsert:
		return applyGenericInsert(ctx, tx, table, metadata, event)
	case eventUpdate:
		return applyGenericUpdate(ctx, tx, table, metadata, event)
	case eventDelete:
		return applyGenericDelete(ctx, tx, table, metadata, event)
	default:
		return fmt.Errorf("tipo de evento nao suportado pelo PostgreSQLSink: %s", event.Type)
	}
}

func (sink *postgresSink) destinationMetadata(
	ctx context.Context,
	tx pgx.Tx,
	table qualifiedTable,
	sourceColumns []string,
) (destinationTableMetadata, error) {
	signature := strings.Join(sourceColumns, "\x00")
	if cached, ok := sink.metadataCache[table.key()]; ok && (signature == "" || cached.sourceSignature == signature) {
		return cached, nil
	}

	rows, err := tx.Query(ctx, `
		SELECT attribute.attname
		FROM pg_catalog.pg_attribute AS attribute
		JOIN pg_catalog.pg_class AS relation ON relation.oid = attribute.attrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = $1
		  AND relation.relname = $2
		  AND relation.relkind IN ('r', 'p')
		  AND attribute.attnum > 0
		  AND NOT attribute.attisdropped
		ORDER BY attribute.attnum
	`, table.schema, table.name)
	if err != nil {
		return destinationTableMetadata{}, fmt.Errorf("consultar colunas de %s no destination: %w", table.key(), err)
	}
	defer rows.Close()

	metadata := destinationTableMetadata{
		columnSet:       make(map[string]struct{}),
		sourceSignature: signature,
	}
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return destinationTableMetadata{}, fmt.Errorf("ler coluna de %s no destination: %w", table.key(), err)
		}
		metadata.columns = append(metadata.columns, column)
		metadata.columnSet[column] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return destinationTableMetadata{}, fmt.Errorf("listar colunas de %s no destination: %w", table.key(), err)
	}
	if len(metadata.columns) == 0 {
		return destinationTableMetadata{}, fmt.Errorf("tabela %s nao existe no destination", table.key())
	}

	pkRows, err := tx.Query(ctx, `
		SELECT attribute.attname
		FROM pg_catalog.pg_class AS relation
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		JOIN pg_catalog.pg_index AS index ON index.indrelid = relation.oid AND index.indisprimary
		JOIN LATERAL unnest(index.indkey) WITH ORDINALITY AS key(attnum, position) ON true
		JOIN pg_catalog.pg_attribute AS attribute
		  ON attribute.attrelid = relation.oid
		 AND attribute.attnum = key.attnum
		WHERE namespace.nspname = $1
		  AND relation.relname = $2
		ORDER BY key.position
	`, table.schema, table.name)
	if err != nil {
		return destinationTableMetadata{}, fmt.Errorf("consultar chave primaria de %s no destination: %w", table.key(), err)
	}
	defer pkRows.Close()
	for pkRows.Next() {
		var column string
		if err := pkRows.Scan(&column); err != nil {
			return destinationTableMetadata{}, fmt.Errorf("ler chave primaria de %s no destination: %w", table.key(), err)
		}
		metadata.primaryKey = append(metadata.primaryKey, column)
	}
	if err := pkRows.Err(); err != nil {
		return destinationTableMetadata{}, fmt.Errorf("listar chave primaria de %s no destination: %w", table.key(), err)
	}
	if len(metadata.primaryKey) == 0 {
		return destinationTableMetadata{}, fmt.Errorf("tabela %s precisa de chave primaria para CDC generico", table.key())
	}

	sink.metadataCache[table.key()] = metadata
	return metadata, nil
}

func validateEventColumns(table qualifiedTable, metadata destinationTableMetadata, event event) error {
	for _, values := range []map[string]any{event.Data, event.OldData} {
		for column := range values {
			if _, exists := metadata.columnSet[column]; !exists {
				return fmt.Errorf("coluna %s.%s nao existe no destination", table.key(), column)
			}
		}
	}
	return nil
}

func applyGenericInsert(ctx context.Context, tx pgx.Tx, table qualifiedTable, metadata destinationTableMetadata, event event) error {
	columns := orderedEventColumns(metadata.columns, event.Data)
	if len(columns) == 0 {
		return fmt.Errorf("INSERT %s sem colunas", table.key())
	}
	values := valuesForColumns(event.Data, columns)
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		pgx.Identifier{table.schema, table.name}.Sanitize(),
		quotedColumns(columns),
		placeholders(len(columns), 1),
	)
	if _, err := tx.Exec(ctx, query, values...); err != nil {
		return fmt.Errorf("aplicar INSERT %s no destination: %w", table.key(), err)
	}
	return nil
}

func applyGenericUpdate(ctx context.Context, tx pgx.Tx, table qualifiedTable, metadata destinationTableMetadata, event event) error {
	columns := orderedEventColumns(metadata.columns, event.Data)
	if len(columns) == 0 {
		return fmt.Errorf("UPDATE %s sem colunas", table.key())
	}
	keyValues, err := primaryKeyValues(metadata.primaryKey, event.OldData, event.Data)
	if err != nil {
		return fmt.Errorf("UPDATE %s: %w", table.key(), err)
	}

	assignments := make([]string, 0, len(columns))
	for index, column := range columns {
		assignments = append(assignments, fmt.Sprintf("%s = $%d", pgx.Identifier{column}.Sanitize(), index+1))
	}
	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		pgx.Identifier{table.schema, table.name}.Sanitize(),
		strings.Join(assignments, ", "),
		keyPredicate(metadata.primaryKey, len(columns)+1),
	)
	values := append(valuesForColumns(event.Data, columns), keyValues...)
	if _, err := tx.Exec(ctx, query, values...); err != nil {
		return fmt.Errorf("aplicar UPDATE %s no destination: %w", table.key(), err)
	}
	return nil
}

func applyGenericDelete(ctx context.Context, tx pgx.Tx, table qualifiedTable, metadata destinationTableMetadata, event event) error {
	keyValues, err := primaryKeyValues(metadata.primaryKey, event.Data, nil)
	if err != nil {
		return fmt.Errorf("DELETE %s: %w", table.key(), err)
	}
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		pgx.Identifier{table.schema, table.name}.Sanitize(),
		keyPredicate(metadata.primaryKey, 1),
	)
	if _, err := tx.Exec(ctx, query, keyValues...); err != nil {
		return fmt.Errorf("aplicar DELETE %s no destination: %w", table.key(), err)
	}
	return nil
}

func orderedEventColumns(destinationOrder []string, values map[string]any) []string {
	columns := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, column := range destinationOrder {
		if _, exists := values[column]; exists {
			columns = append(columns, column)
			seen[column] = struct{}{}
		}
	}
	remaining := make([]string, 0)
	for column := range values {
		if _, exists := seen[column]; !exists {
			remaining = append(remaining, column)
		}
	}
	sort.Strings(remaining)
	return append(columns, remaining...)
}

func valuesForColumns(values map[string]any, columns []string) []any {
	result := make([]any, 0, len(columns))
	for _, column := range columns {
		result = append(result, values[column])
	}
	return result
}

func primaryKeyValues(primaryKey []string, preferred, fallback map[string]any) ([]any, error) {
	values := make([]any, 0, len(primaryKey))
	for _, column := range primaryKey {
		value, exists := preferred[column]
		if !exists {
			value, exists = fallback[column]
		}
		if !exists {
			return nil, fmt.Errorf("valor da chave primaria %q ausente", column)
		}
		values = append(values, value)
	}
	return values, nil
}

func quotedColumns(columns []string) string {
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, pgx.Identifier{column}.Sanitize())
	}
	return strings.Join(quoted, ", ")
}

func placeholders(count, start int) string {
	values := make([]string, count)
	for index := range values {
		values[index] = fmt.Sprintf("$%d", start+index)
	}
	return strings.Join(values, ", ")
}

func keyPredicate(primaryKey []string, start int) string {
	predicates := make([]string, 0, len(primaryKey))
	for index, column := range primaryKey {
		predicates = append(predicates, fmt.Sprintf("%s = $%d", pgx.Identifier{column}.Sanitize(), start+index))
	}
	return strings.Join(predicates, " AND ")
}
