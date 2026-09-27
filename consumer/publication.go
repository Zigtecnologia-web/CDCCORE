package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func reconcilePublication(
	ctx context.Context,
	config *pgx.ConnConfig,
	publication string,
	tables []qualifiedTable,
) error {
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("conectar para configurar publication: %w", err)
	}
	defer conn.Close(context.Background())

	var exists bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", publication).Scan(&exists); err != nil {
		return fmt.Errorf("consultar publication %q: %w", publication, err)
	}
	if !exists {
		identifiers := make([]string, 0, len(tables))
		for _, table := range tables {
			identifiers = append(identifiers, pgx.Identifier{table.schema, table.name}.Sanitize())
		}
		query := fmt.Sprintf(
			"CREATE PUBLICATION %s FOR TABLE %s",
			pgx.Identifier{publication}.Sanitize(),
			joinSQLIdentifiers(identifiers),
		)
		if _, err := conn.Exec(ctx, query); err != nil {
			return fmt.Errorf("criar publication %q: %w", publication, err)
		}
		return nil
	}

	for _, table := range tables {
		var included bool
		if err := conn.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_publication_tables
				WHERE pubname = $1 AND schemaname = $2 AND tablename = $3
			)
		`, publication, table.schema, table.name).Scan(&included); err != nil {
			return fmt.Errorf("consultar tabela %s na publication: %w", table.key(), err)
		}
		if included {
			continue
		}
		query := fmt.Sprintf(
			"ALTER PUBLICATION %s ADD TABLE %s",
			pgx.Identifier{publication}.Sanitize(),
			pgx.Identifier{table.schema, table.name}.Sanitize(),
		)
		if _, err := conn.Exec(ctx, query); err != nil {
			return fmt.Errorf("adicionar %s a publication %q: %w", table.key(), publication, err)
		}
	}
	return nil
}

func joinSQLIdentifiers(identifiers []string) string {
	result := ""
	for index, identifier := range identifiers {
		if index > 0 {
			result += ", "
		}
		result += identifier
	}
	return result
}
