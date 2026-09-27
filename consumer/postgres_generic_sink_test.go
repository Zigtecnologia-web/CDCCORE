package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

func TestGenericPostgresSinkAppliesUnmappedTable(t *testing.T) {
	db := openTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tableName := fmt.Sprintf("cdc_generic_%d", time.Now().UnixNano())
	qualifiedName := "public." + tableName
	quotedName := pgIdentifier("public", tableName)
	if _, err := db.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id BIGINT NOT NULL,
			id BIGINT NOT NULL,
			descricao TEXT NOT NULL,
			observacao TEXT,
			PRIMARY KEY (tenant_id, id)
		)
	`, quotedName)); err != nil {
		t.Fatalf("criar tabela generica: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = db.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", quotedName))
	})

	tables, err := parseTableList(qualifiedName)
	if err != nil {
		t.Fatalf("configurar allowlist: %v", err)
	}
	sourceID := fmt.Sprintf("generic-test-%d", time.Now().UnixNano())
	sink, err := newPostgresSinkWithOptions(ctx, cloneDestinationConfig(t, db), sourceID, postgresSinkOptions{
		applyMode:      "generic",
		includedTables: tables,
	})
	if err != nil {
		t.Fatalf("criar sink generico: %v", err)
	}
	defer sink.Close(context.Background())

	columns := []string{"tenant_id", "id", "descricao", "observacao"}
	insert := sourceTransaction{
		sourceID: sourceID, commitLSN: pglogrepl.LSN(0x501), transactionID: 1,
		events: []event{{
			Type: eventInsert, Schema: "public", Table: tableName, Columns: columns,
			Data: map[string]any{"tenant_id": int64(7), "id": int64(10), "descricao": "inserido", "observacao": nil},
		}},
	}
	if _, err := sink.ApplyTransaction(ctx, insert); err != nil {
		t.Fatalf("aplicar insert generico: %v", err)
	}
	if _, err := sink.ApplyTransaction(ctx, insert); err != nil {
		t.Fatalf("reaplicar transacao generica: %v", err)
	}
	assertGenericRow(t, db, quotedName, 7, 10, "inserido")

	if _, err := sink.ApplyTransaction(ctx, sourceTransaction{
		sourceID: sourceID, commitLSN: pglogrepl.LSN(0x502), transactionID: 2,
		events: []event{{
			Type: eventUpdate, Schema: "public", Table: tableName, Columns: columns,
			Data:    map[string]any{"tenant_id": int64(7), "id": int64(11), "descricao": "atualizado", "observacao": "ok"},
			OldData: map[string]any{"tenant_id": int64(7), "id": int64(10)},
		}},
	}); err != nil {
		t.Fatalf("aplicar update generico: %v", err)
	}
	assertGenericRow(t, db, quotedName, 7, 11, "atualizado")

	if _, err := sink.ApplyTransaction(ctx, sourceTransaction{
		sourceID: sourceID, commitLSN: pglogrepl.LSN(0x503), transactionID: 3,
		events: []event{{
			Type: eventDelete, Schema: "public", Table: tableName, Columns: columns,
			OldData: map[string]any{"tenant_id": int64(7), "id": int64(11)},
		}},
	}); err != nil {
		t.Fatalf("aplicar delete generico: %v", err)
	}

	var count int
	if err := db.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", quotedName)).Scan(&count); err != nil {
		t.Fatalf("contar linhas genericas: %v", err)
	}
	if count != 0 {
		t.Fatalf("delete generico deixou %d linha(s)", count)
	}
}

func TestGenericPostgresSinkRejectsTableOutsideAllowlist(t *testing.T) {
	db := openTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sink, err := newPostgresSinkWithOptions(ctx, cloneDestinationConfig(t, db), "generic-allowlist-test", postgresSinkOptions{
		applyMode:      "generic",
		includedTables: []qualifiedTable{{schema: "public", name: "clientes"}},
	})
	if err != nil {
		t.Fatalf("criar sink generico: %v", err)
	}
	defer sink.Close(context.Background())

	_, err = sink.ApplyTransaction(ctx, sourceTransaction{
		sourceID: "generic-allowlist-test", commitLSN: pglogrepl.LSN(0x601), transactionID: 1,
		events: []event{{
			Type: eventInsert, Schema: "public", Table: "enderecos",
			Data: map[string]any{"id": int64(1)},
		}},
	})
	if err == nil {
		t.Fatal("tabela fora da allowlist deveria ser rejeitada")
	}
}

func TestGenericPostgresSinkRejectsTableWithoutPrimaryKey(t *testing.T) {
	db := openTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tableName := fmt.Sprintf("cdc_without_pk_%d", time.Now().UnixNano())
	quotedName := pgIdentifier("public", tableName)
	if _, err := db.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id BIGINT, descricao TEXT)", quotedName)); err != nil {
		t.Fatalf("criar tabela sem chave: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = db.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", quotedName))
	})

	sink, err := newPostgresSinkWithOptions(ctx, cloneDestinationConfig(t, db), "generic-no-pk-test", postgresSinkOptions{
		applyMode:      "generic",
		includedTables: []qualifiedTable{{schema: "public", name: tableName}},
	})
	if err != nil {
		t.Fatalf("criar sink generico: %v", err)
	}
	defer sink.Close(context.Background())

	_, err = sink.ApplyTransaction(ctx, sourceTransaction{
		sourceID: "generic-no-pk-test", commitLSN: pglogrepl.LSN(0x701), transactionID: 1,
		events: []event{{
			Type: eventInsert, Schema: "public", Table: tableName,
			Data: map[string]any{"id": int64(1), "descricao": "sem chave"},
		}},
	})
	if err == nil {
		t.Fatal("tabela sem chave primaria deveria ser rejeitada")
	}
}

func TestParseTableListRequiresQualifiedNames(t *testing.T) {
	if _, err := parseTableList("clientes"); err == nil {
		t.Fatal("nome sem schema deveria ser rejeitado")
	}
	tables, err := parseTableList(" public.clientes, public.enderecos,public.clientes ")
	if err != nil {
		t.Fatalf("lista valida rejeitada: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("esperava duas tabelas sem duplicacao, recebeu %d", len(tables))
	}
}

func assertGenericRow(t *testing.T, db *pgx.Conn, table string, tenantID, id int64, expected string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var description string
	query := fmt.Sprintf("SELECT descricao FROM %s WHERE tenant_id = $1 AND id = $2", table)
	if err := db.QueryRow(ctx, query, tenantID, id).Scan(&description); err != nil {
		t.Fatalf("consultar linha generica: %v", err)
	}
	if description != expected {
		t.Fatalf("descricao inesperada: esperado=%q atual=%q", expected, description)
	}
}

func pgIdentifier(parts ...string) string {
	return pgx.Identifier(parts).Sanitize()
}
