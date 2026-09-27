package main

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestReconcilePublicationCreatesAndAddsTables(t *testing.T) {
	db := openTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	publication := fmt.Sprintf("cdc_test_publication_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = db.Exec(cleanupCtx, "DROP PUBLICATION IF EXISTS "+pgIdentifier(publication))
	})

	config := cloneDestinationConfig(t, db)
	if err := reconcilePublication(ctx, config, publication, []qualifiedTable{{schema: "public", name: "clientes"}}); err != nil {
		t.Fatalf("criar publication automaticamente: %v", err)
	}
	if err := reconcilePublication(ctx, config, publication, []qualifiedTable{
		{schema: "public", name: "clientes"},
		{schema: "public", name: "enderecos"},
	}); err != nil {
		t.Fatalf("adicionar tabela automaticamente: %v", err)
	}

	var count int
	if err := db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_publication_tables
		WHERE pubname = $1
		  AND schemaname = 'public'
		  AND tablename IN ('clientes', 'enderecos')
	`, publication).Scan(&count); err != nil {
		t.Fatalf("consultar publication de teste: %v", err)
	}
	if count != 2 {
		t.Fatalf("esperava duas tabelas na publication, recebeu %d", count)
	}
}
