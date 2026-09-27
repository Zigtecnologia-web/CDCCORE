package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

type sourceTransaction struct {
	sourceID      string
	commitLSN     pglogrepl.LSN
	transactionID uint32
	events        []event
}

type sink interface {
	ApplyTransaction(context.Context, sourceTransaction) ([]string, error)
	Close(context.Context) error
	Name() string
	Ready(context.Context) error
}

type fileSink struct {
	file  *os.File
	match string
}

func newFileSink(filename string) (*fileSink, error) {
	output, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("abrir arquivo %q: %w", filename, err)
	}
	return &fileSink{file: output, match: os.Getenv("CDC_TEST_FAIL_WRITE_MATCH")}, nil
}

func (sink *fileSink) Name() string {
	return sink.file.Name()
}

func (sink *fileSink) Ready(context.Context) error {
	return nil
}

func (sink *fileSink) Close(context.Context) error {
	return sink.file.Close()
}

func (sink *fileSink) ApplyTransaction(_ context.Context, tx sourceTransaction) ([]string, error) {
	persisted := make([]string, 0, len(tx.events))
	for _, event := range tx.events {
		event.LSN = tx.commitLSN.String()
		line, err := encodeEvent(event)
		if err != nil {
			return nil, err
		}
		fmt.Println(line)
		if sink.match != "" && strings.Contains(line, sink.match) {
			return nil, fmt.Errorf("falha de escrita injetada para teste: %q", sink.match)
		}
		if _, err := sink.file.WriteString(line + "\n"); err != nil {
			return nil, fmt.Errorf("gravar evento no arquivo %q: %w", sink.file.Name(), err)
		}
		persisted = append(persisted, line)
	}
	return persisted, nil
}

type postgresSink struct {
	config         *pgx.ConnConfig
	conn           *pgx.Conn
	sourceID       string
	applyMode      string
	includedTables map[string]struct{}
	metadataCache  map[string]destinationTableMetadata
}

type postgresSinkOptions struct {
	applyMode      string
	includedTables []qualifiedTable
}

func newPostgresSink(ctx context.Context, config *pgx.ConnConfig, sourceID string) (*postgresSink, error) {
	return newPostgresSinkWithOptions(ctx, config, sourceID, postgresSinkOptions{applyMode: defaultPostgresApplyMode})
}

func newPostgresSinkWithOptions(
	ctx context.Context,
	config *pgx.ConnConfig,
	sourceID string,
	options postgresSinkOptions,
) (*postgresSink, error) {
	includedTables := make(map[string]struct{}, len(options.includedTables))
	for _, table := range options.includedTables {
		includedTables[table.key()] = struct{}{}
	}
	sink := &postgresSink{
		config:         config,
		sourceID:       sourceID,
		applyMode:      options.applyMode,
		includedTables: includedTables,
		metadataCache:  make(map[string]destinationTableMetadata),
	}
	if err := sink.connect(ctx); err != nil {
		return nil, err
	}
	if err := sink.ensureMetadata(ctx); err != nil {
		_ = sink.Close(context.Background())
		return nil, err
	}
	return sink, nil
}

func (sink *postgresSink) Name() string {
	return fmt.Sprintf("postgres:%s", sink.applyMode)
}

func (sink *postgresSink) Ready(ctx context.Context) error {
	if sink.conn == nil {
		return errors.New("destination desconectado")
	}
	return sink.conn.Ping(ctx)
}

func (sink *postgresSink) Close(ctx context.Context) error {
	if sink.conn == nil {
		return nil
	}
	err := sink.conn.Close(ctx)
	sink.conn = nil
	return err
}

func (sink *postgresSink) connect(ctx context.Context) error {
	conn, err := pgx.ConnectConfig(ctx, sink.config)
	if err != nil {
		return fmt.Errorf("conexao destination PostgreSQL: %w", err)
	}
	sink.conn = conn
	return nil
}

func (sink *postgresSink) ensureMetadata(ctx context.Context) error {
	_, err := sink.conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS cdc_applied_transactions (
			source_id TEXT NOT NULL,
			commit_lsn PG_LSN NOT NULL,
			transaction_id BIGINT,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (source_id, commit_lsn)
		)
	`)
	if err != nil {
		return fmt.Errorf("criar metadata de idempotencia no destination: %w", err)
	}
	return nil
}

func (sink *postgresSink) ApplyTransaction(ctx context.Context, sourceTx sourceTransaction) ([]string, error) {
	if err := sink.Ready(ctx); err != nil {
		if reconnectErr := sink.reconnect(ctx); reconnectErr != nil {
			return nil, reconnectErr
		}
	}

	tx, err := sink.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("iniciar transacao no destination: %w", err)
	}
	defer tx.Rollback(context.Background())

	var alreadyApplied bool
	err = tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM cdc_applied_transactions
			WHERE source_id = $1
			  AND commit_lsn = $2::pg_lsn
		)
	`, sourceTx.sourceID, sourceTx.commitLSN.String()).Scan(&alreadyApplied)
	if err != nil {
		return nil, fmt.Errorf("verificar idempotencia no destination: %w", err)
	}
	if alreadyApplied {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("confirmar transacao ja aplicada no destination: %w", err)
		}
		return nil, nil
	}

	for _, event := range sourceTx.events {
		if sink.applyMode == "generic" {
			if err := sink.applyGenericEvent(ctx, tx, event); err != nil {
				return nil, err
			}
		} else {
			if err := applyExplicitEvent(ctx, tx, event); err != nil {
				return nil, err
			}
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO cdc_applied_transactions (source_id, commit_lsn, transaction_id)
		VALUES ($1, $2::pg_lsn, $3)
	`, sourceTx.sourceID, sourceTx.commitLSN.String(), int64(sourceTx.transactionID))
	if err != nil {
		return nil, fmt.Errorf("registrar transacao aplicada no destination: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit no destination: %w", err)
	}
	return nil, nil
}

func (sink *postgresSink) reconnect(ctx context.Context) error {
	_ = sink.Close(context.Background())
	if err := sink.connect(ctx); err != nil {
		return err
	}
	sink.metadataCache = make(map[string]destinationTableMetadata)
	return sink.ensureMetadata(ctx)
}

func applyExplicitEvent(ctx context.Context, tx pgx.Tx, event event) error {
	if event.Schema != "public" {
		return fmt.Errorf("PostgreSQLSink nao suporta o schema %s", event.Schema)
	}
	switch event.Table {
	case "clientes":
		return applyClienteEvent(ctx, tx, event)
	case "enderecos":
		return applyEnderecoEvent(ctx, tx, event)
	default:
		return fmt.Errorf("PostgreSQLSink nao suporta public.%s", event.Table)
	}
}

func applyClienteEvent(ctx context.Context, tx pgx.Tx, event event) error {
	switch event.Type {
	case eventInsert:
		_, err := tx.Exec(ctx,
			"INSERT INTO public.clientes (id, nome, email) VALUES ($1, $2, $3)",
			requiredValue(event.Data, "id"), requiredValue(event.Data, "nome"), event.Data["email"],
		)
		if err != nil {
			return fmt.Errorf("aplicar INSERT public.clientes no destination: %w", err)
		}
	case eventUpdate:
		_, err := tx.Exec(ctx,
			"UPDATE public.clientes SET nome = $1, email = $2 WHERE id = $3",
			requiredValue(event.Data, "nome"), event.Data["email"], requiredValue(event.Data, "id"),
		)
		if err != nil {
			return fmt.Errorf("aplicar UPDATE public.clientes no destination: %w", err)
		}
	case eventDelete:
		id := requiredValue(event.OldData, "id")
		_, err := tx.Exec(ctx, "DELETE FROM public.clientes WHERE id = $1", id)
		if err != nil {
			return fmt.Errorf("aplicar DELETE public.clientes no destination: %w", err)
		}
	default:
		return fmt.Errorf("tipo de evento nao suportado pelo PostgreSQLSink: %s", event.Type)
	}
	return nil
}

func applyEnderecoEvent(ctx context.Context, tx pgx.Tx, event event) error {
	switch event.Type {
	case eventInsert:
		_, err := tx.Exec(ctx, `
			INSERT INTO public.enderecos
				(id, cliente_id, logradouro, numero, complemento, bairro, cidade, estado, cep)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		`,
			requiredValue(event.Data, "id"), requiredValue(event.Data, "cliente_id"),
			requiredValue(event.Data, "logradouro"), event.Data["numero"], event.Data["complemento"],
			event.Data["bairro"], requiredValue(event.Data, "cidade"),
			requiredValue(event.Data, "estado"), event.Data["cep"],
		)
		if err != nil {
			return fmt.Errorf("aplicar INSERT public.enderecos no destination: %w", err)
		}
	case eventUpdate:
		_, err := tx.Exec(ctx, `
			UPDATE public.enderecos
			SET cliente_id = $1, logradouro = $2, numero = $3, complemento = $4,
				bairro = $5, cidade = $6, estado = $7, cep = $8
			WHERE id = $9
		`,
			requiredValue(event.Data, "cliente_id"), requiredValue(event.Data, "logradouro"),
			event.Data["numero"], event.Data["complemento"], event.Data["bairro"],
			requiredValue(event.Data, "cidade"), requiredValue(event.Data, "estado"),
			event.Data["cep"], requiredValue(event.Data, "id"),
		)
		if err != nil {
			return fmt.Errorf("aplicar UPDATE public.enderecos no destination: %w", err)
		}
	case eventDelete:
		_, err := tx.Exec(ctx, "DELETE FROM public.enderecos WHERE id = $1", requiredValue(event.OldData, "id"))
		if err != nil {
			return fmt.Errorf("aplicar DELETE public.enderecos no destination: %w", err)
		}
	default:
		return fmt.Errorf("tipo de evento nao suportado pelo PostgreSQLSink: %s", event.Type)
	}
	return nil
}

func requiredValue(values map[string]any, name string) any {
	if values == nil {
		return nil
	}
	return values[name]
}
