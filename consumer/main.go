package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/joho/godotenv"
)

const (
	defaultSlotName       = "cdc_slot"
	defaultPublication    = "cdc_publication"
	expectedPlugin        = "pgoutput"
	pgoutputProtoVersion  = "1"
	defaultOutputFileName = "cdc.txt"
	defaultStatusInterval = 10 * time.Second
	shutdownTimeout       = 3 * time.Second
	testPauseLogMessage   = "pausado antes do ACK para teste"
)

type slotState struct {
	plugin       string
	confirmedLSN pglogrepl.LSN
	active       bool
}

type cdcState struct {
	receivedLSN  pglogrepl.LSN // WALStart of the last pgoutput frame received.
	serverWALEnd pglogrepl.LSN // Current server WAL end advertised with that frame.
	processedLSN pglogrepl.LSN // Last source transaction committed successfully by the sink.
	confirmedLSN pglogrepl.LSN // Last processed position sent in a StandbyStatusUpdate.
}

func (state *cdcState) advanceProcessedLSN(candidate pglogrepl.LSN) error {
	if candidate < state.processedLSN {
		return fmt.Errorf("processed LSN regrediu de %s para %s", state.processedLSN, candidate)
	}
	state.processedLSN = candidate
	return nil
}

type eventType string

const (
	eventInsert eventType = "INSERT"
	eventUpdate eventType = "UPDATE"
	eventDelete eventType = "DELETE"
)

type event struct {
	Type          eventType      `json:"type"`
	LSN           string         `json:"lsn"`
	TransactionID uint32         `json:"transaction_id"`
	Schema        string         `json:"schema"`
	Table         string         `json:"table"`
	Data          map[string]any `json:"data,omitempty"`
	OldData       map[string]any `json:"old_data,omitempty"`
	Columns       []string       `json:"-"`
}

type relationRegistry map[uint32]*pglogrepl.RelationMessage

type transactionState struct {
	id                uint32
	beginLSN          pglogrepl.LSN
	events            []event
	schemaValidations []schemaValidationResult
	schemaChanges     []schemaChange
}

type pgoutputDecoder struct {
	relations relationRegistry
	schema    schemaState
	validator schemaValidator
	audit     *schemaAuditLogger
	types     *pgtype.Map
	tx        *transactionState
	sourceID  string
}

func newPgoutputDecoder(sourceID string) *pgoutputDecoder {
	return newPgoutputDecoderWithValidator(sourceID, nil)
}

func newPgoutputDecoderWithValidator(sourceID string, validator schemaValidator) *pgoutputDecoder {
	return &pgoutputDecoder{
		relations: relationRegistry{},
		schema:    schemaState{},
		validator: validator,
		types:     pgtype.NewMap(),
		sourceID:  sourceID,
	}
}

func main() {
	log.SetFlags(log.Ltime)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		log.Printf("erro: %v", err)
		os.Exit(1)
	}

	log.Println("consumidor encerrado")
}

func run(ctx context.Context) (runErr error) {
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("carregar arquivo .env: %w", err)
	}

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	metrics := newMetrics()
	readiness := &readinessState{}
	if cfg.enableHealth {
		startHealthServer(ctx, cfg.healthAddr, metrics, readiness)
	}

	attempt := 0
	for {
		err := runOnce(ctx, cfg, metrics, readiness)
		if err == nil || ctx.Err() != nil {
			return err
		}
		readiness.setNotReady(err)
		attempt++
		if cfg.retry.maxAttempts > 0 && attempt >= cfg.retry.maxAttempts {
			return err
		}
		delay := backoffDelay(cfg.retry, attempt)
		log.Printf("level=warn event=retry error=%q attempt=%d delay=%s", err.Error(), attempt, delay)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		metrics.reconnects.Add(1)
	}
}

func runOnce(ctx context.Context, cfg appConfig, metrics *metrics, readiness *readinessState) (runErr error) {
	if cfg.publicationAutoConfigure {
		if err := reconcilePublication(ctx, cfg.sourceConfig, cfg.publication, cfg.includedTables); err != nil {
			return err
		}
	}
	state, err := readSlotState(ctx, cfg.sourceConfig, cfg.slotName)
	if err != nil {
		return err
	}
	if state.plugin != expectedPlugin {
		return fmt.Errorf("o slot %q usa o plugin %q; esperado %q", cfg.slotName, state.plugin, expectedPlugin)
	}
	if state.active {
		return fmt.Errorf("o slot %q ja esta ativo; encerre o outro consumidor", cfg.slotName)
	}

	output, err := openSink(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		if err := output.Close(context.Background()); err != nil && runErr == nil {
			runErr = fmt.Errorf("fechar sink %q: %w", output.Name(), err)
		}
	}()
	var validator schemaValidator
	if cfg.sinkName == "postgres" {
		validator, err = newPostgresSchemaValidator(ctx, cfg.sourceConfig, cfg.destConfig, cfg.includedTables)
		if err != nil {
			return err
		}
		defer func() {
			if err := validator.Close(context.Background()); err != nil && runErr == nil {
				runErr = fmt.Errorf("fechar validator de schema: %w", err)
			}
		}()
	}
	var auditCatalog *schemaCatalogReader
	if cfg.schemaAuditLog {
		auditConn, err := pgx.ConnectConfig(ctx, cfg.sourceConfig)
		if err != nil {
			return fmt.Errorf("conexao source para auditoria de schema: %w", err)
		}
		auditCatalog = &schemaCatalogReader{name: "source-audit", conn: auditConn}
		defer func() {
			if err := auditConn.Close(context.Background()); err != nil && runErr == nil {
				runErr = fmt.Errorf("fechar conexao de auditoria de schema: %w", err)
			}
		}()
	}

	replicationConfig := cfg.sourceConfig.Config.Copy()
	replicationConfig.RuntimeParams["replication"] = "database"
	replicationConfig.RuntimeParams["application_name"] = "cdccore"

	conn, err := pgconn.ConnectConfig(ctx, replicationConfig)
	if err != nil {
		return fmt.Errorf("conexao de replicacao com PostgreSQL: %w", err)
	}
	defer closeReplicationConnection(conn)

	log.Printf(
		"conectado a %s:%d/%s como %s",
		cfg.sourceConfig.Host,
		cfg.sourceConfig.Port,
		cfg.sourceConfig.Database,
		cfg.sourceConfig.User,
	)
	log.Printf("iniciando slot %q no LSN confirmado %s", cfg.slotName, state.confirmedLSN)
	log.Printf("usando publication %q", cfg.publication)
	log.Printf("usando sink %q", output.Name())

	if err := pglogrepl.StartReplication(
		ctx,
		conn,
		cfg.slotName,
		state.confirmedLSN,
		pglogrepl.StartReplicationOptions{
			Mode:       pglogrepl.LogicalReplication,
			PluginArgs: pgoutputPluginArgs(cfg.publication),
		},
	); err != nil {
		return fmt.Errorf("iniciar replicacao no slot %q: %w", cfg.slotName, err)
	}

	readiness.setReady()
	log.Println("aguardando eventos; pressione Ctrl+C para encerrar")
	return consume(ctx, conn, output, validator, auditCatalog, state.confirmedLSN, cfg.statusInterval, cfg.pauseBeforeAck, cfg.pauseMatch, cfg.sourceID, metrics, readiness)
}

func pgoutputPluginArgs(publication string) []string {
	return []string{
		fmt.Sprintf("proto_version '%s'", pgoutputProtoVersion),
		fmt.Sprintf("publication_names '%s'", publication),
	}
}

func readSlotState(ctx context.Context, config *pgx.ConnConfig, slotName string) (slotState, error) {
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return slotState{}, fmt.Errorf("conexao com PostgreSQL: %w", err)
	}
	defer conn.Close(context.Background())

	var state slotState
	var lsnText string
	err = conn.QueryRow(ctx, `
		SELECT plugin, confirmed_flush_lsn::text, active
		FROM pg_replication_slots
		WHERE slot_name = $1
		  AND slot_type = 'logical'
		  AND database = current_database()
	`, slotName).Scan(&state.plugin, &lsnText, &state.active)
	if errors.Is(err, pgx.ErrNoRows) {
		return slotState{}, fmt.Errorf(
			"slot logico %q nao encontrado no banco %q; crie-o com ./scripts/create-slot.sh",
			slotName,
			config.Database,
		)
	}
	if err != nil {
		return slotState{}, fmt.Errorf("consultar slot %q: %w", slotName, err)
	}

	state.confirmedLSN, err = pglogrepl.ParseLSN(lsnText)
	if err != nil {
		return slotState{}, fmt.Errorf("LSN confirmado invalido %q: %w", lsnText, err)
	}

	return state, nil
}

func consume(
	ctx context.Context,
	conn *pgconn.PgConn,
	output sink,
	validator schemaValidator,
	auditCatalog *schemaCatalogReader,
	startLSN pglogrepl.LSN,
	statusInterval time.Duration,
	pauseBeforeAck bool,
	pauseMatch string,
	sourceID string,
	metrics *metrics,
	readiness *readinessState,
) error {
	state := cdcState{
		processedLSN: startLSN,
		confirmedLSN: startLSN,
	}
	metrics.setProcessed(startLSN)
	metrics.setConfirmed(startLSN)
	decoder := newPgoutputDecoderWithValidator(sourceID, validator)
	if auditCatalog != nil {
		decoder.audit = &schemaAuditLogger{catalog: auditCatalog}
	}
	nextStatus := time.Now().Add(statusInterval)
	pauseArmed := false

	for {
		receiveCtx, cancel := context.WithDeadline(ctx, nextStatus)
		message, err := conn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				if !pauseBeforeAck {
					_ = sendFinalStatus(conn, state.processedLSN)
				}
				return nil
			}
			if pgconn.Timeout(err) {
				if !pauseBeforeAck {
					if err := sendProcessedStatus(ctx, conn, &state); err != nil {
						readiness.setNotReady(err)
						return err
					}
					metrics.setConfirmed(state.confirmedLSN)
				}
				nextStatus = time.Now().Add(statusInterval)
				continue
			}
			readiness.setNotReady(err)
			return fmt.Errorf("conexao de replicacao perdida: %w", err)
		}

		if errorResponse, ok := message.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("erro do PostgreSQL no stream de replicacao: %s", errorResponse.Message)
		}

		copyData, ok := message.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.XLogDataByteID:
			xlogData, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("decodificar mensagem WAL: %w", err)
			}

			state.receivedLSN = xlogData.WALStart
			state.serverWALEnd = xlogData.ServerWALEnd
			commitLSN, persisted, err := decoder.process(ctx, xlogData.WALData, xlogData.WALStart, xlogData.ServerWALEnd, output, metrics)
			if err != nil {
				metrics.sinkErrors.Add(1)
				readiness.setNotReady(err)
				return err
			}
			for _, persistedEvent := range persisted {
				if pauseBeforeAck && (pauseMatch == "" || strings.Contains(persistedEvent, pauseMatch)) {
					pauseArmed = true
				}
			}
			if commitLSN > 0 {
				if err := state.advanceProcessedLSN(commitLSN); err != nil {
					readiness.setNotReady(err)
					return err
				}
				metrics.setProcessed(commitLSN)
				if pauseArmed {
					log.Printf("%s no LSN %s", testPauseLogMessage, state.processedLSN)
					<-ctx.Done()
					return nil
				}
			}

		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("decodificar keepalive: %w", err)
			}
			if keepalive.ReplyRequested && !pauseBeforeAck {
				if err := sendProcessedStatus(ctx, conn, &state); err != nil {
					readiness.setNotReady(err)
					return err
				}
				metrics.setConfirmed(state.confirmedLSN)
				nextStatus = time.Now().Add(statusInterval)
			}
		}

		if time.Now().After(nextStatus) && !pauseBeforeAck {
			if err := sendProcessedStatus(ctx, conn, &state); err != nil {
				readiness.setNotReady(err)
				return err
			}
			metrics.setConfirmed(state.confirmedLSN)
			nextStatus = time.Now().Add(statusInterval)
		}
	}
}

func (decoder *pgoutputDecoder) process(
	ctx context.Context,
	walData []byte,
	messageLSN pglogrepl.LSN,
	serverWALEnd pglogrepl.LSN,
	output sink,
	metrics *metrics,
) (pglogrepl.LSN, []string, error) {
	// pgoutput protocol v1 emits one logical message per OutputPluginWrite,
	// which the walsender wraps in one XLogData CopyData frame.
	message, err := pglogrepl.Parse(walData)
	if err != nil {
		return 0, nil, fmt.Errorf("decodificar mensagem pgoutput: %w", err)
	}
	return decoder.processMessageWithServerWALEnd(ctx, message, messageLSN, serverWALEnd, output, metrics)
}

func (decoder *pgoutputDecoder) processMessage(
	ctx context.Context,
	message pglogrepl.Message,
	messageLSN pglogrepl.LSN,
	output sink,
	metrics *metrics,
) (pglogrepl.LSN, []string, error) {
	return decoder.processMessageWithServerWALEnd(ctx, message, messageLSN, 0, output, metrics)
}

func (decoder *pgoutputDecoder) processMessageWithServerWALEnd(
	ctx context.Context,
	message pglogrepl.Message,
	messageLSN pglogrepl.LSN,
	serverWALEnd pglogrepl.LSN,
	output sink,
	metrics *metrics,
) (pglogrepl.LSN, []string, error) {
	if decoder.audit != nil {
		decoder.audit.logMessage(ctx, message, messageLSN, serverWALEnd, decoder.currentTransactionID())
	}
	switch message := message.(type) {
	case *pglogrepl.BeginMessage:
		if decoder.tx != nil {
			return 0, nil, fmt.Errorf("BEGIN xid=%d recebido com transacao xid=%d ainda ativa", message.Xid, decoder.tx.id)
		}
		decoder.tx = &transactionState{
			id:       message.Xid,
			beginLSN: message.FinalLSN,
		}
		metrics.transactionsReceived.Add(1)
		log.Printf("BEGIN xid=%d lsn=%s", message.Xid, message.FinalLSN)
	case *pglogrepl.RelationMessage:
		if metrics != nil {
			metrics.schemaMetadataRelationMessages.Add(1)
		}
		var xid uint32
		schemaLSN := messageLSN
		if decoder.tx != nil {
			xid = decoder.tx.id
			if schemaLSN == 0 {
				schemaLSN = decoder.tx.beginLSN
			}
		}
		changes, err := decoder.schema.observeRelation(message, schemaLSN, xid)
		if err != nil {
			return 0, nil, fmt.Errorf("observar metadata da relacao: %w", err)
		}
		decoder.relations[message.RelationID] = message
		if len(changes) > 0 {
			logSchemaChanges(decoder.sourceID, changes, metrics)
		}
		table := qualifiedTable{schema: message.Namespace, name: message.RelationName}
		if decoder.validator != nil && decoder.validator.ShouldValidate(table) {
			result := decoder.validator.Validate(ctx, table, schemaLSN, xid)
			if decoder.tx != nil {
				decoder.tx.schemaValidations = append(decoder.tx.schemaValidations, result)
			}
		}
		log.Printf("RELATION id=%d table=%s.%s", message.RelationID, message.Namespace, message.RelationName)
	case *pglogrepl.InsertMessage:
		event, err := decoder.eventFromTuple(eventInsert, message.RelationID, message.Tuple, nil, messageLSN)
		if err != nil {
			return 0, nil, err
		}
		if err := decoder.appendEvent(event); err != nil {
			return 0, nil, err
		}
		metrics.eventsReceived.Add(1)
	case *pglogrepl.UpdateMessage:
		event, err := decoder.updateEvent(message, messageLSN)
		if err != nil {
			return 0, nil, err
		}
		if err := decoder.appendEvent(event); err != nil {
			return 0, nil, err
		}
		metrics.eventsReceived.Add(1)
	case *pglogrepl.DeleteMessage:
		event, err := decoder.eventFromTuple(eventDelete, message.RelationID, message.OldTuple, nil, messageLSN)
		if err != nil {
			return 0, nil, err
		}
		if err := decoder.appendEvent(event); err != nil {
			return 0, nil, err
		}
		metrics.eventsReceived.Add(1)
	case *pglogrepl.CommitMessage:
		persisted, err := decoder.commit(ctx, message, output, metrics)
		if err != nil {
			metrics.transactionsFailed.Add(1)
			return 0, nil, err
		}
		return message.TransactionEndLSN, persisted, nil
	case *pglogrepl.TypeMessage, *pglogrepl.OriginMessage:
		// Metadata messages are acknowledged only after a later processed position.
	default:
		log.Printf("mensagem pgoutput ignorada: %T", message)
	}

	return 0, nil, nil
}

func (decoder *pgoutputDecoder) currentTransactionID() uint32 {
	if decoder.tx == nil {
		return 0
	}
	return decoder.tx.id
}

func (decoder *pgoutputDecoder) appendEvent(event event) error {
	if decoder.tx == nil {
		return fmt.Errorf("evento %s recebido sem transacao ativa", event.Type)
	}
	event.TransactionID = decoder.tx.id
	if err := validateChangeEvent(event); err != nil {
		return err
	}
	decoder.tx.events = append(decoder.tx.events, event)
	return nil
}

func validateChangeEvent(event event) error {
	if event.LSN == "" {
		return fmt.Errorf("evento %s sem lsn", event.Type)
	}
	if _, err := pglogrepl.ParseLSN(event.LSN); err != nil {
		return fmt.Errorf("evento %s com lsn invalido %q: %w", event.Type, event.LSN, err)
	}
	if event.TransactionID == 0 {
		return fmt.Errorf("evento %s sem transaction_id", event.Type)
	}
	if event.Schema == "" {
		return fmt.Errorf("evento %s sem schema", event.Type)
	}
	if event.Table == "" {
		return fmt.Errorf("evento %s sem table", event.Type)
	}

	switch event.Type {
	case eventInsert:
		if event.Data == nil {
			return fmt.Errorf("evento INSERT sem data")
		}
		if event.OldData != nil {
			return fmt.Errorf("evento INSERT nao deve possuir old_data")
		}
	case eventUpdate:
		if event.Data == nil {
			return fmt.Errorf("evento UPDATE sem data")
		}
		if event.OldData == nil {
			return fmt.Errorf("evento UPDATE sem old_data; configure REPLICA IDENTITY FULL na tabela source")
		}
	case eventDelete:
		if event.Data == nil {
			return fmt.Errorf("evento DELETE sem data; configure REPLICA IDENTITY FULL na tabela source")
		}
		if event.OldData != nil {
			return fmt.Errorf("evento DELETE nao deve possuir old_data")
		}
	default:
		return fmt.Errorf("tipo de evento nao suportado: %q", event.Type)
	}

	return nil
}

func (decoder *pgoutputDecoder) commit(ctx context.Context, message *pglogrepl.CommitMessage, output sink, metrics *metrics) ([]string, error) {
	if decoder.tx == nil {
		return nil, fmt.Errorf("COMMIT sem transacao ativa lsn=%s", message.TransactionEndLSN)
	}
	defer func() {
		decoder.tx = nil
	}()

	sourceTx := sourceTransaction{
		sourceID:          decoder.sourceID,
		commitLSN:         message.TransactionEndLSN,
		transactionID:     decoder.tx.id,
		schemaValidations: append([]schemaValidationResult(nil), decoder.tx.schemaValidations...),
		schemaChanges:     append([]schemaChange(nil), decoder.tx.schemaChanges...),
		events:            append([]event(nil), decoder.tx.events...),
		metrics:           metrics,
	}
	persisted, err := output.ApplyTransaction(ctx, sourceTx)
	if err != nil {
		return nil, err
	}
	metrics.eventsApplied.Add(uint64(len(decoder.tx.events)))
	metrics.transactionsCommitted.Add(1)
	log.Printf("level=info event=transaction_committed source_id=%s xid=%d commit_lsn=%s eventos=%d", decoder.sourceID, decoder.tx.id, message.TransactionEndLSN, len(decoder.tx.events))
	return persisted, nil
}

func encodeEvent(event event) (string, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("codificar evento CDC em JSON: %w", err)
	}
	return string(data), nil
}

func (decoder *pgoutputDecoder) updateEvent(message *pglogrepl.UpdateMessage, lsn pglogrepl.LSN) (event, error) {
	var oldData *pglogrepl.TupleData
	if message.OldTuple != nil {
		oldData = message.OldTuple
	}
	return decoder.eventFromTuple(eventUpdate, message.RelationID, message.NewTuple, oldData, lsn)
}

func (decoder *pgoutputDecoder) eventFromTuple(
	eventType eventType,
	relationID uint32,
	dataTuple *pglogrepl.TupleData,
	oldTuple *pglogrepl.TupleData,
	lsn pglogrepl.LSN,
) (event, error) {
	relation, ok := decoder.relations[relationID]
	if !ok {
		return event{}, fmt.Errorf("relacao %d desconhecida para evento %s", relationID, eventType)
	}

	event := event{
		Type:    eventType,
		LSN:     lsn.String(),
		Schema:  relation.Namespace,
		Table:   relation.RelationName,
		Columns: make([]string, 0, len(relation.Columns)),
	}
	for _, column := range relation.Columns {
		event.Columns = append(event.Columns, column.Name)
	}

	if dataTuple != nil {
		data, err := decoder.tupleToMap(relation, dataTuple)
		if err != nil {
			return event, err
		}
		event.Data = data
	}
	if oldTuple != nil {
		oldData, err := decoder.tupleToMap(relation, oldTuple)
		if err != nil {
			return event, err
		}
		event.OldData = oldData
	}

	return event, nil
}

func (decoder *pgoutputDecoder) tupleToMap(relation *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (map[string]any, error) {
	if len(tuple.Columns) > len(relation.Columns) {
		return nil, fmt.Errorf(
			"tupla possui %d colunas, mas relacao %s.%s possui %d",
			len(tuple.Columns),
			relation.Namespace,
			relation.RelationName,
			len(relation.Columns),
		)
	}

	values := make(map[string]any, len(tuple.Columns))
	for index, column := range tuple.Columns {
		relationColumn := relation.Columns[index]
		switch column.DataType {
		case pglogrepl.TupleDataTypeNull:
			values[relationColumn.Name] = nil
		case pglogrepl.TupleDataTypeToast:
			continue
		case pglogrepl.TupleDataTypeText:
			value, err := decoder.decodeTextColumn(column.Data, relationColumn.DataType)
			if err != nil {
				return nil, fmt.Errorf("decodificar coluna %s.%s.%s: %w", relation.Namespace, relation.RelationName, relationColumn.Name, err)
			}
			values[relationColumn.Name] = value
		case pglogrepl.TupleDataTypeBinary:
			values[relationColumn.Name] = string(column.Data)
		default:
			return nil, fmt.Errorf("tipo de dado de tupla desconhecido %q na coluna %s", column.DataType, relationColumn.Name)
		}
	}

	return values, nil
}

func (decoder *pgoutputDecoder) decodeTextColumn(data []byte, dataType uint32) (any, error) {
	if dataTypeInfo, ok := decoder.types.TypeForOID(dataType); ok {
		return dataTypeInfo.Codec.DecodeValue(decoder.types, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func durationFromEnv(name string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("%s deve ser uma duracao positiva: %q", name, value)
	}
	return duration, nil
}

func boolFromEnv(name string, fallback bool) (bool, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s deve ser true ou false: %q", name, value)
	}
	return parsed, nil
}

func openSink(ctx context.Context, cfg appConfig) (sink, error) {
	switch cfg.sinkName {
	case "file":
		return newFileSink(cfg.outputFileName)
	case "postgres":
		return newPostgresSinkWithOptions(ctx, cfg.destConfig, cfg.sourceID, postgresSinkOptions{
			applyMode:       cfg.postgresApplyMode,
			schemaEvolution: cfg.schemaEvolution,
			includedTables:  cfg.includedTables,
		})
	default:
		return nil, fmt.Errorf("sink nao suportado: %s", cfg.sinkName)
	}
}

func backoffDelay(cfg retryConfig, attempt int) time.Duration {
	delay := cfg.initialDelay
	for index := 1; index < attempt; index++ {
		delay *= 2
		if delay >= cfg.maxDelay {
			return cfg.maxDelay
		}
	}
	if delay > cfg.maxDelay {
		return cfg.maxDelay
	}
	return delay
}

func sendStatus(ctx context.Context, conn *pgconn.PgConn, lsn pglogrepl.LSN) error {
	// This is PostgreSQL replication-position confirmation, not a business-message ACK.
	// The three positions are intentionally aligned with the last processed LSN.
	err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
		ClientTime:       time.Now(),
	})
	if err != nil {
		return fmt.Errorf("confirmar LSN %s ao PostgreSQL: %w", lsn, err)
	}
	return nil
}

func sendProcessedStatus(ctx context.Context, conn *pgconn.PgConn, state *cdcState) error {
	if err := sendStatus(ctx, conn, state.processedLSN); err != nil {
		return err
	}
	advanced := state.processedLSN > state.confirmedLSN
	state.confirmedLSN = state.processedLSN
	if advanced {
		log.Printf("level=debug event=ack_sent confirmed_lsn=%s", state.confirmedLSN)
	}
	return nil
}

func sendFinalStatus(conn *pgconn.PgConn, processedLSN pglogrepl.LSN) error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return sendStatus(ctx, conn, processedLSN)
}

func closeReplicationConnection(conn *pgconn.PgConn) {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	_ = conn.Close(ctx)
}
