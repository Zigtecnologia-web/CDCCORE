package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/joho/godotenv"
)

const (
	testSlotName = "cdc_slot"
	testTimeout  = 30 * time.Second
	pollInterval = 25 * time.Millisecond
)

var consumerBinaryPath string

func TestMain(m *testing.M) {
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(os.Stderr, "carregar .env para os testes: %v\n", err)
		os.Exit(1)
	}

	buildDir, err := os.MkdirTemp("", "cdc-consumer-integration-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "criar diretorio temporario: %v\n", err)
		os.Exit(1)
	}

	consumerBinaryPath = filepath.Join(buildDir, "cdc-consumer")
	build := exec.Command("go", "build", "-o", consumerBinaryPath, ".")
	if output, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "compilar consumidor: %v\n%s", err, output)
		_ = os.RemoveAll(buildDir)
		os.Exit(1)
	}

	code := m.Run()
	_ = os.RemoveAll(buildDir)
	os.Exit(code)
}

func TestCDCRecovery(t *testing.T) {
	db := openTestDatabase(t)
	requireSlotInactive(t, db)

	name, email := testIdentity("CDC_TEST_RECOVERY")
	cleanupEmail(t, db, email)
	before := readSlot(t, db).confirmedLSN
	execSQL(t, db, "INSERT INTO clientes (nome, email) VALUES ($1, $2)", name, email)

	if afterInsert := readSlot(t, db).confirmedLSN; afterInsert != before {
		t.Fatalf("LSN confirmado avancou sem consumidor: antes=%s depois=%s", before, afterInsert)
	}

	outputFile := filepath.Join(t.TempDir(), "cdc.txt")
	consumer := startConsumer(t, db, outputFile, nil)
	waitFileContains(t, outputFile, name)
	waitForAcknowledgement(t, db, eventLSN(t, outputFile, name))
	consumer.stop(t, db, false)
}

func TestCDCCrashBeforeAck(t *testing.T) {
	db := openTestDatabase(t)
	requireSlotInactive(t, db)

	name, email := testIdentity("CDC_TEST_CRASH")
	cleanupEmail(t, db, email)
	before := readSlot(t, db).confirmedLSN

	firstOutput := filepath.Join(t.TempDir(), "before-crash.txt")
	consumer := startConsumer(t, db, firstOutput, map[string]string{
		"CDC_TEST_PAUSE_BEFORE_ACK": "true",
		"CDC_TEST_PAUSE_MATCH":      name,
	})
	execSQL(t, db, "INSERT INTO clientes (nome, email) VALUES ($1, $2)", name, email)
	waitFileContains(t, firstOutput, name)
	waitConsumerOutput(t, consumer, testPauseLogMessage)

	if duringPause := readSlot(t, db).confirmedLSN; duringPause != before {
		t.Fatalf("LSN foi confirmado antes do crash: antes=%s durante_pausa=%s", before, duringPause)
	}
	consumer.stop(t, db, true)

	secondOutput := filepath.Join(t.TempDir(), "after-crash.txt")
	restarted := startConsumer(t, db, secondOutput, nil)
	waitFileContains(t, secondOutput, name)
	waitForAcknowledgement(t, db, eventLSN(t, secondOutput, name))
	restarted.stop(t, db, false)
}

func TestCDCAfterAck(t *testing.T) {
	db := openTestDatabase(t)
	requireSlotInactive(t, db)

	name, email := testIdentity("CDC_TEST_ACK")
	cleanupEmail(t, db, email)
	firstOutput := filepath.Join(t.TempDir(), "before-restart.txt")
	consumer := startConsumer(t, db, firstOutput, nil)
	execSQL(t, db, "INSERT INTO clientes (nome, email) VALUES ($1, $2)", name, email)
	waitFileContains(t, firstOutput, name)
	waitForAcknowledgement(t, db, eventLSN(t, firstOutput, name))
	consumer.stop(t, db, false)

	secondOutput := filepath.Join(t.TempDir(), "after-restart.txt")
	restarted := startConsumer(t, db, secondOutput, nil)
	barrier, barrierEmail := testIdentity("CDC_TEST_ACK_BARRIER")
	execTransaction(t, db,
		"INSERT INTO clientes (nome, email) VALUES ($1, $2)", []any{barrier, barrierEmail},
		"DELETE FROM clientes WHERE email = $1", []any{barrierEmail},
	)
	waitFileContains(t, secondOutput, barrier)
	waitForAcknowledgement(t, db, eventLSN(t, secondOutput, barrier))

	content := readOutputFile(t, secondOutput)
	if strings.Contains(content, name) {
		t.Fatalf("evento confirmado foi entregue novamente apos reinicio:\n%s", content)
	}
	restarted.stop(t, db, false)
}

func TestCDCEventOrder(t *testing.T) {
	db := openTestDatabase(t)
	requireSlotInactive(t, db)

	names := make([]string, 3)
	emails := make([]string, 3)
	for index := range names {
		name, email := testIdentity(fmt.Sprintf("CDC_TEST_ORDER_%d", index+1))
		names[index] = name
		emails[index] = email
		cleanupEmail(t, db, email)
	}

	outputFile := filepath.Join(t.TempDir(), "order.txt")
	consumer := startConsumer(t, db, outputFile, nil)
	confirmed := readSlot(t, db).confirmedLSN
	for index, name := range names {
		execSQL(t, db, "INSERT INTO clientes (nome, email) VALUES ($1, $2)", name, emails[index])
		waitFileContains(t, outputFile, name)
		waitForAcknowledgement(t, db, eventLSN(t, outputFile, name))
		nextConfirmed := readSlot(t, db).confirmedLSN
		if nextConfirmed <= confirmed {
			t.Fatalf("LSN confirmado nao avancou apos %q: antes=%s depois=%s", name, confirmed, nextConfirmed)
		}
		confirmed = nextConfirmed
	}

	content := readOutputFile(t, outputFile)
	first := strings.Index(content, names[0])
	second := strings.Index(content, names[1])
	third := strings.Index(content, names[2])
	if !(first < second && second < third) {
		t.Fatalf("eventos fora de ordem: indices=%d,%d,%d\n%s", first, second, third, content)
	}
	consumer.stop(t, db, false)
}

func TestCDCWriteFailureDoesNotAdvanceCheckpoint(t *testing.T) {
	db := openTestDatabase(t)
	requireSlotInactive(t, db)

	firstName, firstEmail := testIdentity("CDC_TEST_WRITE_OK")
	secondName, secondEmail := testIdentity("CDC_TEST_WRITE_FAIL")
	cleanupEmail(t, db, firstEmail)
	cleanupEmail(t, db, secondEmail)

	firstOutput := filepath.Join(t.TempDir(), "write-failure.txt")
	consumer := startConsumer(t, db, firstOutput, map[string]string{
		"CDC_TEST_FAIL_WRITE_MATCH": secondName,
		"CDC_RETRY_MAX_ATTEMPTS":    "1",
	})

	execSQL(t, db, "INSERT INTO clientes (nome, email) VALUES ($1, $2)", firstName, firstEmail)
	waitFileContains(t, firstOutput, firstName)
	waitForAcknowledgement(t, db, eventLSN(t, firstOutput, firstName))
	beforeFailure := readSlot(t, db).confirmedLSN

	execSQL(t, db, "INSERT INTO clientes (nome, email) VALUES ($1, $2)", secondName, secondEmail)
	waitConsumerDone(t, consumer)
	if consumer.error() == nil {
		t.Fatalf("consumidor deveria encerrar com erro de escrita:\n%s", consumer.output.String())
	}
	afterFailure := readSlot(t, db).confirmedLSN
	if afterFailure != beforeFailure {
		t.Fatalf("LSN confirmado avancou apos falha de escrita: antes=%s depois=%s", beforeFailure, afterFailure)
	}
	if content := readOutputFile(t, firstOutput); strings.Contains(content, secondName) {
		t.Fatalf("evento com escrita falha apareceu no arquivo:\n%s", content)
	}
	consumer.stop(t, db, true)
	waitForSlotInactive(t, db)

	secondOutput := filepath.Join(t.TempDir(), "after-write-failure.txt")
	restarted := startConsumer(t, db, secondOutput, nil)
	waitFileContains(t, secondOutput, secondName)
	waitForAcknowledgement(t, db, eventLSN(t, secondOutput, secondName))
	restarted.stop(t, db, false)
}

func TestCDCTransactionOrder(t *testing.T) {
	db := openTestDatabase(t)
	requireSlotInactive(t, db)

	firstName, firstEmail := testIdentity("CDC_TEST_TX_1")
	secondName, secondEmail := testIdentity("CDC_TEST_TX_2")
	cleanupEmail(t, db, firstEmail)
	cleanupEmail(t, db, secondEmail)

	outputFile := filepath.Join(t.TempDir(), "transaction.txt")
	consumer := startConsumer(t, db, outputFile, nil)
	execTransaction(t, db,
		"INSERT INTO clientes (nome, email) VALUES ($1, $2)", []any{firstName, firstEmail},
		"INSERT INTO clientes (nome, email) VALUES ($1, $2)", []any{secondName, secondEmail},
	)
	waitFileContains(t, outputFile, firstName, secondName)

	events := readEvents(t, outputFile)
	firstIndex := eventIndex(events, firstName)
	secondIndex := eventIndex(events, secondName)
	if firstIndex < 0 || secondIndex != firstIndex+1 {
		t.Fatalf("ordem esperada INSERT, INSERT nao preservada: %+v", events)
	}
	if events[firstIndex].TransactionID == 0 || events[firstIndex].TransactionID != events[secondIndex].TransactionID {
		t.Fatalf("eventos da transacao sem mesmo transaction_id: %+v", events)
	}

	waitForAcknowledgement(t, db, eventLSN(t, outputFile, secondName))
	consumer.stop(t, db, false)
}

func TestCDCRealTransactionPreservesProtocolOrder(t *testing.T) {
	db := openTestDatabase(t)
	requireSlotInactive(t, db)

	name, email := testIdentity("CDC_TEST_PROTOCOL_ORDER")
	updatedName := name + "_UPDATED"
	cleanupEmail(t, db, email)

	outputFile := filepath.Join(t.TempDir(), "protocol-order.jsonl")
	consumer := startConsumer(t, db, outputFile, nil)
	xid := execInsertUpdateDeleteTransaction(t, db, name, updatedName, email)
	waitFileContains(t, outputFile, name, updatedName, email)

	var matching []recordedEvent
	waitUntil(t, testTimeout, "INSERT, UPDATE e DELETE da mesma transacao", func() (bool, error) {
		matching = matching[:0]
		for _, event := range readEventsIfExists(t, outputFile) {
			if eventContains(event, email) {
				matching = append(matching, event)
			}
		}
		return len(matching) == 3, nil
	})

	want := []eventType{eventInsert, eventUpdate, eventDelete}
	for index, event := range matching {
		if event.Type != want[index] {
			t.Fatalf("ordem real inesperada no indice %d: esperado=%s atual=%s", index, want[index], event.Type)
		}
		if uint64(event.TransactionID) != xid {
			t.Fatalf("evento fora da transacao %d: %+v", xid, event)
		}
	}
	if matching[0].LSN != matching[1].LSN || matching[1].LSN != matching[2].LSN {
		t.Fatalf("eventos da mesma transacao receberam LSNs diferentes: %+v", matching)
	}

	waitForAcknowledgement(t, db, eventLSN(t, outputFile, email))
	waitConsumerOutput(t, consumer, fmt.Sprintf("BEGIN xid=%d", xid))
	waitConsumerOutput(t, consumer, fmt.Sprintf("xid=%d commit_lsn=", xid))
	consumer.stop(t, db, false)
}

func TestCDCConsecutiveTransactionsRemainSeparated(t *testing.T) {
	db := openTestDatabase(t)
	requireSlotInactive(t, db)

	firstName, firstEmail := testIdentity("CDC_TEST_CONSECUTIVE_1")
	secondName, secondEmail := testIdentity("CDC_TEST_CONSECUTIVE_2")
	cleanupEmail(t, db, firstEmail)
	cleanupEmail(t, db, secondEmail)

	outputFile := filepath.Join(t.TempDir(), "consecutive.jsonl")
	consumer := startConsumer(t, db, outputFile, nil)
	execSQL(t, db, "INSERT INTO clientes (nome, email) VALUES ($1, $2)", firstName, firstEmail)
	execSQL(t, db, "INSERT INTO clientes (nome, email) VALUES ($1, $2)", secondName, secondEmail)
	waitFileContains(t, outputFile, firstName, secondName)

	events := readEvents(t, outputFile)
	firstIndex := eventIndex(events, firstName)
	secondIndex := eventIndex(events, secondName)
	if firstIndex < 0 || secondIndex <= firstIndex {
		t.Fatalf("ordem das transacoes consecutivas nao preservada: %+v", events)
	}
	first := events[firstIndex]
	second := events[secondIndex]
	if first.TransactionID == 0 || second.TransactionID == 0 || first.TransactionID == second.TransactionID {
		t.Fatalf("fronteiras transacionais incorretas: primeira=%+v segunda=%+v", first, second)
	}
	firstLSN, err := pglogrepl.ParseLSN(first.LSN)
	if err != nil {
		t.Fatalf("LSN da primeira transacao invalido: %v", err)
	}
	secondLSN, err := pglogrepl.ParseLSN(second.LSN)
	if err != nil {
		t.Fatalf("LSN da segunda transacao invalido: %v", err)
	}
	if secondLSN <= firstLSN {
		t.Fatalf("LSNs das transacoes nao avancaram: primeira=%s segunda=%s", firstLSN, secondLSN)
	}
	waitForAcknowledgement(t, db, secondLSN)
	consumer.stop(t, db, false)
}

func TestCDCStructuredInsertUpdateDelete(t *testing.T) {
	db := openTestDatabase(t)
	requireSlotInactive(t, db)

	name, email := testIdentity("CDC_TEST_STRUCTURED")
	cleanupEmail(t, db, email)

	outputFile := filepath.Join(t.TempDir(), "structured.jsonl")
	consumer := startConsumer(t, db, outputFile, nil)

	execSQL(t, db, "INSERT INTO clientes (nome, email) VALUES ($1, $2)", name, email)
	waitFileContains(t, outputFile, name)
	insertEvent := waitForEvent(t, outputFile, name, eventInsert)
	assertCommonEvent(t, insertEvent)
	if insertEvent.Data["nome"] != name || insertEvent.Data["email"] != email {
		t.Fatalf("INSERT sem valores esperados: %+v", insertEvent)
	}
	if _, ok := insertEvent.Data["id"]; !ok {
		t.Fatalf("INSERT sem id: %+v", insertEvent)
	}

	updatedName := name + "_ATUALIZADO"
	execSQL(t, db, "UPDATE clientes SET nome = $1 WHERE email = $2", updatedName, email)
	waitFileContains(t, outputFile, updatedName)
	updateEvent := waitForEvent(t, outputFile, updatedName, eventUpdate)
	assertCommonEvent(t, updateEvent)
	if updateEvent.Data["nome"] != updatedName || updateEvent.Data["email"] != email {
		t.Fatalf("UPDATE sem valores novos esperados: %+v", updateEvent)
	}
	if updateEvent.OldData["nome"] != name {
		t.Fatalf("UPDATE sem old_data esperado: %+v", updateEvent)
	}

	execSQL(t, db, "DELETE FROM clientes WHERE email = $1", email)
	waitUntil(t, testTimeout, "DELETE estruturado", func() (bool, error) {
		for _, event := range readEventsIfExists(t, outputFile) {
			if event.Type == eventDelete && event.OldData["email"] == email {
				return true, nil
			}
		}
		return false, nil
	})
	deleteEvent := waitForEvent(t, outputFile, email, eventDelete)
	assertCommonEvent(t, deleteEvent)
	if deleteEvent.OldData["nome"] != updatedName {
		t.Fatalf("DELETE sem old_data esperado: %+v", deleteEvent)
	}

	waitForAcknowledgement(t, db, eventLSN(t, outputFile, email))
	consumer.stop(t, db, false)
}

func TestCDCRollbackDoesNotPersistBusinessEvent(t *testing.T) {
	db := openTestDatabase(t)
	requireSlotInactive(t, db)

	name, email := testIdentity("CDC_TEST_ROLLBACK")
	cleanupEmail(t, db, email)

	outputFile := filepath.Join(t.TempDir(), "rollback.jsonl")
	consumer := startConsumer(t, db, outputFile, nil)
	execRollbackTransaction(t, db, "INSERT INTO clientes (nome, email) VALUES ($1, $2)", name, email)

	time.Sleep(300 * time.Millisecond)
	if content := readOutputFileIfExists(t, outputFile); strings.Contains(content, name) {
		t.Fatalf("evento de transacao abortada foi persistido:\n%s", content)
	}
	consumer.stop(t, db, false)
}

func TestCDCUnicode(t *testing.T) {
	db := openTestDatabase(t)
	requireSlotInactive(t, db)

	values := []string{"José", "João da Silva", "Ação", "Educação", "São Paulo"}
	outputFile := filepath.Join(t.TempDir(), "unicode.jsonl")
	consumer := startConsumer(t, db, outputFile, nil)

	for _, value := range values {
		name := value + "_" + strconv.FormatInt(time.Now().UnixNano(), 10)
		email := strings.ToLower(strconv.FormatInt(time.Now().UnixNano(), 10)) + "@unicode.test"
		cleanupEmail(t, db, email)
		execSQL(t, db, "INSERT INTO clientes (nome, email) VALUES ($1, $2)", name, email)
		waitFileContains(t, outputFile, name)
		event := waitForEvent(t, outputFile, name, eventInsert)
		if event.Data["nome"] != name {
			t.Fatalf("unicode nao preservado: esperado=%q evento=%+v", name, event)
		}
	}

	consumer.stop(t, db, false)
}

func TestPostgreSQLSinkInsertUpdateDeleteAndIdempotency(t *testing.T) {
	db := openTestDatabase(t)
	config := cloneDestinationConfig(t, db)
	id := int64(time.Now().UnixNano() % 1_000_000_000)
	email := fmt.Sprintf("sink_%d@teste.com", id)
	sourceID := fmt.Sprintf("sink-test-%d", id)
	cleanupEmail(t, db, email)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sink, err := newPostgresSink(ctx, config, sourceID)
	if err != nil {
		t.Fatalf("criar PostgreSQLSink: %v", err)
	}
	defer sink.Close(context.Background())

	insertTx := sourceTransaction{
		sourceID:      sourceID,
		commitLSN:     pglogrepl.LSN(0x101),
		transactionID: 1,
		events: []event{{
			Type:   eventInsert,
			Schema: "public",
			Table:  "clientes",
			Data:   map[string]any{"id": id, "nome": "Sink Insert", "email": email},
		}},
	}
	if _, err := sink.ApplyTransaction(ctx, insertTx); err != nil {
		t.Fatalf("aplicar insert: %v", err)
	}
	if _, err := sink.ApplyTransaction(ctx, insertTx); err != nil {
		t.Fatalf("reaplicar insert idempotente: %v", err)
	}
	assertClienteName(t, db, email, "Sink Insert")

	updateTx := sourceTransaction{
		sourceID:      sourceID,
		commitLSN:     pglogrepl.LSN(0x102),
		transactionID: 2,
		events: []event{{
			Type:   eventUpdate,
			Schema: "public",
			Table:  "clientes",
			Data:   map[string]any{"id": id, "nome": "Sink Update", "email": email},
		}},
	}
	if _, err := sink.ApplyTransaction(ctx, updateTx); err != nil {
		t.Fatalf("aplicar update: %v", err)
	}
	assertClienteName(t, db, email, "Sink Update")

	deleteTx := sourceTransaction{
		sourceID:      sourceID,
		commitLSN:     pglogrepl.LSN(0x103),
		transactionID: 3,
		events: []event{{
			Type:    eventDelete,
			Schema:  "public",
			Table:   "clientes",
			OldData: map[string]any{"id": id, "nome": "Sink Update", "email": email},
		}},
	}
	if _, err := sink.ApplyTransaction(ctx, deleteTx); err != nil {
		t.Fatalf("aplicar delete: %v", err)
	}
	assertClienteMissing(t, db, email)
}

func TestPostgreSQLSinkRollsBackTransactionOnError(t *testing.T) {
	db := openTestDatabase(t)
	config := cloneDestinationConfig(t, db)
	id := int64(time.Now().UnixNano() % 1_000_000_000)
	email := fmt.Sprintf("sink_rollback_%d@teste.com", id)
	sourceID := fmt.Sprintf("sink-rollback-test-%d", id)
	cleanupEmail(t, db, email)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sink, err := newPostgresSink(ctx, config, sourceID)
	if err != nil {
		t.Fatalf("criar PostgreSQLSink: %v", err)
	}
	defer sink.Close(context.Background())

	_, err = sink.ApplyTransaction(ctx, sourceTransaction{
		sourceID:      sourceID,
		commitLSN:     pglogrepl.LSN(0x201),
		transactionID: 1,
		events: []event{
			{Type: eventInsert, Schema: "public", Table: "clientes", Data: map[string]any{"id": id, "nome": "Vai Rollback", "email": email}},
			{Type: eventInsert, Schema: "public", Table: "outra_tabela", Data: map[string]any{"id": id, "nome": "Falha", "email": email}},
		},
	})
	if err == nil {
		t.Fatalf("transacao deveria falhar")
	}
	assertClienteMissing(t, db, email)
}

func TestPostgreSQLSinkUnavailable(t *testing.T) {
	db := openTestDatabase(t)
	base := db.Config()
	config, err := pgx.ParseConfig(fmt.Sprintf("host=127.0.0.1 port=1 dbname=%s user=%s password=invalid connect_timeout=1 sslmode=disable", base.Database, base.User))
	if err != nil {
		t.Fatalf("montar config indisponivel: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := newPostgresSink(ctx, config, "sink-unavailable-test"); err == nil {
		t.Fatalf("destination indisponivel deveria falhar")
	}
}

func TestRelationRegistryBuildsInsertEvent(t *testing.T) {
	decoder := newPgoutputDecoder("unit-test")
	decoder.relations[7] = &pglogrepl.RelationMessage{
		RelationID:   7,
		Namespace:    "public",
		RelationName: "clientes",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int8OID},
			{Name: "nome", DataType: pgtype.TextOID},
			{Name: "email", DataType: pgtype.TextOID},
		},
	}
	decoder.tx = &transactionState{id: 42}

	event, err := decoder.eventFromTuple(eventInsert, 7, &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("10")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("Maria")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("maria@example.com")},
		},
	}, nil, 123)
	if err != nil {
		t.Fatalf("montar evento: %v", err)
	}
	if event.Schema != "public" || event.Table != "clientes" || event.Data["nome"] != "Maria" {
		t.Fatalf("evento inesperado: %+v", event)
	}
	if event.Data["id"] != int64(10) {
		t.Fatalf("id nao decodificado como int64: %#v", event.Data["id"])
	}
}

func TestDecoderPreservesEventOrder(t *testing.T) {
	decoder := decoderWithClientesRelation()
	output := &recordingSink{}
	metrics := newMetrics()
	ctx := context.Background()

	messages := []pglogrepl.Message{
		&pglogrepl.BeginMessage{FinalLSN: 0x100, Xid: 42},
		&pglogrepl.InsertMessage{RelationID: 7, Tuple: clientesTuple("1", "Inicial", "ordem@teste.com")},
		&pglogrepl.UpdateMessage{RelationID: 7, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld, OldTuple: clientesTuple("1", "Inicial", "ordem@teste.com"), NewTuple: clientesTuple("1", "Atualizado", "ordem@teste.com")},
		&pglogrepl.DeleteMessage{RelationID: 7, OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: clientesTuple("1", "Atualizado", "ordem@teste.com")},
		&pglogrepl.CommitMessage{CommitLSN: 0x120, TransactionEndLSN: 0x128},
	}

	var processed pglogrepl.LSN
	for index, message := range messages {
		commitLSN, _, err := decoder.processMessage(ctx, message, pglogrepl.LSN(0x100+index), output, metrics)
		if err != nil {
			t.Fatalf("processar mensagem %d (%T): %v", index, message, err)
		}
		if commitLSN > 0 {
			processed = commitLSN
		}
	}

	if processed != 0x128 {
		t.Fatalf("LSN processado inesperado: %s", processed)
	}
	if len(output.transactions) != 1 {
		t.Fatalf("esperada uma transacao no sink, recebidas %d", len(output.transactions))
	}
	got := output.transactions[0].events
	want := []eventType{eventInsert, eventUpdate, eventDelete}
	if len(got) != len(want) {
		t.Fatalf("quantidade de eventos inesperada: %+v", got)
	}
	for index := range want {
		if got[index].Type != want[index] {
			t.Fatalf("ordem inesperada no indice %d: esperado=%s atual=%s", index, want[index], got[index].Type)
		}
	}
}

func TestDecoderKeepsTransactionAcrossXLogData(t *testing.T) {
	decoder := decoderWithClientesRelation()
	output := &recordingSink{}
	metrics := newMetrics()
	ctx := context.Background()

	frames := [][]byte{
		beginPayload(0x200, 84),
		insertPayload(7, "10", "Primeiro", "primeiro@teste.com"),
		insertPayload(7, "11", "Segundo", "segundo@teste.com"),
		commitPayload(0x220, 0x228),
	}
	for index, frame := range frames {
		commitLSN, _, err := decoder.process(ctx, frame, pglogrepl.LSN(0x200+index), output, metrics)
		if err != nil {
			t.Fatalf("processar XLogData %d: %v", index+1, err)
		}
		if index < len(frames)-1 && commitLSN != 0 {
			t.Fatalf("XLogData %d avancou LSN antes do COMMIT: %s", index+1, commitLSN)
		}
		if index < len(frames)-1 && len(output.transactions) != 0 {
			t.Fatalf("XLogData %d aplicou transacao incompleta", index+1)
		}
		if index == len(frames)-1 && commitLSN != 0x228 {
			t.Fatalf("TransactionEndLSN inesperado: %s", commitLSN)
		}
	}

	if len(output.transactions) != 1 || len(output.transactions[0].events) != 2 {
		t.Fatalf("transacao atravessando frames nao foi aplicada como unidade: %+v", output.transactions)
	}
}

func TestDecoderProcessesTransactionsSerially(t *testing.T) {
	decoder := decoderWithClientesRelation()
	output := &recordingSink{}
	metrics := newMetrics()
	ctx := context.Background()

	for index, transactionID := range []uint32{101, 102} {
		endLSN := pglogrepl.LSN(0x300 + index*0x20)
		messages := []pglogrepl.Message{
			&pglogrepl.BeginMessage{FinalLSN: endLSN - 8, Xid: transactionID},
			&pglogrepl.InsertMessage{RelationID: 7, Tuple: clientesTuple(fmt.Sprint(index+1), fmt.Sprintf("TX%d", index+1), fmt.Sprintf("tx%d@teste.com", index+1))},
			&pglogrepl.CommitMessage{CommitLSN: endLSN - 8, TransactionEndLSN: endLSN},
		}
		for _, message := range messages {
			if _, _, err := decoder.processMessage(ctx, message, endLSN-8, output, metrics); err != nil {
				t.Fatalf("processar transacao %d: %v", transactionID, err)
			}
		}
	}

	if len(output.transactions) != 2 {
		t.Fatalf("esperadas duas transacoes, recebidas %d", len(output.transactions))
	}
	if output.transactions[0].transactionID != 101 || output.transactions[1].transactionID != 102 {
		t.Fatalf("ordem das transacoes nao preservada: %+v", output.transactions)
	}
	if output.transactions[0].commitLSN >= output.transactions[1].commitLSN {
		t.Fatalf("LSNs de commit nao sao crescentes: %s, %s", output.transactions[0].commitLSN, output.transactions[1].commitLSN)
	}
}

func TestDecoderRejectsMessagesOutsideTransaction(t *testing.T) {
	decoder := decoderWithClientesRelation()
	output := &recordingSink{}
	metrics := newMetrics()

	_, _, err := decoder.processMessage(context.Background(), &pglogrepl.InsertMessage{
		RelationID: 7,
		Tuple:      clientesTuple("1", "Sem Begin", "sem-begin@teste.com"),
	}, 0x400, output, metrics)
	if err == nil {
		t.Fatal("INSERT sem BEGIN deveria falhar")
	}
	if len(output.transactions) != 0 {
		t.Fatal("mensagem sem BEGIN nao deveria chegar ao sink")
	}

	_, _, err = decoder.processMessage(context.Background(), &pglogrepl.CommitMessage{
		TransactionEndLSN: 0x408,
	}, 0x408, output, metrics)
	if err == nil {
		t.Fatal("COMMIT sem BEGIN deveria falhar")
	}
}

func TestDecoderRejectsBeginWithActiveTransaction(t *testing.T) {
	decoder := decoderWithClientesRelation()
	output := &recordingSink{}
	metrics := newMetrics()
	ctx := context.Background()

	if _, _, err := decoder.processMessage(ctx, &pglogrepl.BeginMessage{FinalLSN: 0x450, Xid: 10}, 0x440, output, metrics); err != nil {
		t.Fatalf("primeiro BEGIN: %v", err)
	}
	if _, _, err := decoder.processMessage(ctx, &pglogrepl.BeginMessage{FinalLSN: 0x470, Xid: 11}, 0x460, output, metrics); err == nil {
		t.Fatal("segundo BEGIN com transacao ativa deveria falhar")
	}
	if decoder.tx == nil || decoder.tx.id != 10 {
		t.Fatalf("segundo BEGIN alterou a transacao ativa: %+v", decoder.tx)
	}
}

func TestDecoderDoesNotReturnProcessedLSNWhenSinkFails(t *testing.T) {
	decoder := decoderWithClientesRelation()
	output := &recordingSink{err: errors.New("destination indisponivel")}
	metrics := newMetrics()
	ctx := context.Background()

	messages := []pglogrepl.Message{
		&pglogrepl.BeginMessage{FinalLSN: 0x480, Xid: 12},
		&pglogrepl.InsertMessage{RelationID: 7, Tuple: clientesTuple("12", "Falha", "falha@teste.com")},
	}
	for _, message := range messages {
		if _, _, err := decoder.processMessage(ctx, message, 0x480, output, metrics); err != nil {
			t.Fatalf("preparar transacao: %v", err)
		}
	}
	processedLSN, _, err := decoder.processMessage(ctx, &pglogrepl.CommitMessage{
		CommitLSN:         0x490,
		TransactionEndLSN: 0x498,
	}, 0x490, output, metrics)
	if err == nil {
		t.Fatal("falha do sink deveria ser propagada")
	}
	if processedLSN != 0 {
		t.Fatalf("falha do sink retornou LSN processado: %s", processedLSN)
	}
	if len(output.transactions) != 0 {
		t.Fatalf("sink com falha registrou transacao: %+v", output.transactions)
	}
}

func TestProcessedLSNNeverRegresses(t *testing.T) {
	state := cdcState{processedLSN: 0x500}
	if err := state.advanceProcessedLSN(0x520); err != nil {
		t.Fatalf("avancar processedLSN: %v", err)
	}
	if err := state.advanceProcessedLSN(0x510); err == nil {
		t.Fatal("regressao de processedLSN deveria falhar")
	}
	if state.processedLSN != 0x520 {
		t.Fatalf("processedLSN mudou apos regressao: %s", state.processedLSN)
	}
}

type recordingSink struct {
	transactions []sourceTransaction
	err          error
}

func (sink *recordingSink) ApplyTransaction(_ context.Context, transaction sourceTransaction) ([]string, error) {
	if sink.err != nil {
		return nil, sink.err
	}
	transaction.events = append([]event(nil), transaction.events...)
	sink.transactions = append(sink.transactions, transaction)
	return nil, nil
}

func (*recordingSink) Close(context.Context) error { return nil }
func (*recordingSink) Name() string                { return "recording" }
func (*recordingSink) Ready(context.Context) error { return nil }

func decoderWithClientesRelation() *pgoutputDecoder {
	decoder := newPgoutputDecoder("unit-test")
	decoder.relations[7] = &pglogrepl.RelationMessage{
		RelationID:   7,
		Namespace:    "public",
		RelationName: "clientes",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int8OID},
			{Name: "nome", DataType: pgtype.TextOID},
			{Name: "email", DataType: pgtype.TextOID},
		},
	}
	return decoder
}

func clientesTuple(id, nome, email string) *pglogrepl.TupleData {
	values := []string{id, nome, email}
	columns := make([]*pglogrepl.TupleDataColumn, 0, len(values))
	for _, value := range values {
		columns = append(columns, &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(value)})
	}
	return &pglogrepl.TupleData{ColumnNum: uint16(len(columns)), Columns: columns}
}

func beginPayload(finalLSN pglogrepl.LSN, transactionID uint32) []byte {
	payload := []byte{byte(pglogrepl.MessageTypeBegin)}
	payload = binary.BigEndian.AppendUint64(payload, uint64(finalLSN))
	payload = binary.BigEndian.AppendUint64(payload, 0)
	payload = binary.BigEndian.AppendUint32(payload, transactionID)
	return payload
}

func insertPayload(relationID uint32, values ...string) []byte {
	payload := []byte{byte(pglogrepl.MessageTypeInsert)}
	payload = binary.BigEndian.AppendUint32(payload, relationID)
	payload = append(payload, byte('N'))
	payload = binary.BigEndian.AppendUint16(payload, uint16(len(values)))
	for _, value := range values {
		payload = append(payload, byte(pglogrepl.TupleDataTypeText))
		payload = binary.BigEndian.AppendUint32(payload, uint32(len(value)))
		payload = append(payload, value...)
	}
	return payload
}

func commitPayload(commitLSN, transactionEndLSN pglogrepl.LSN) []byte {
	payload := []byte{byte(pglogrepl.MessageTypeCommit), 0}
	payload = binary.BigEndian.AppendUint64(payload, uint64(commitLSN))
	payload = binary.BigEndian.AppendUint64(payload, uint64(transactionEndLSN))
	payload = binary.BigEndian.AppendUint64(payload, 0)
	return payload
}

type databaseSlot struct {
	confirmedLSN pglogrepl.LSN
	active       bool
}

type recordedEvent struct {
	Type          eventType      `json:"type"`
	LSN           string         `json:"lsn"`
	TransactionID uint32         `json:"transaction_id"`
	Schema        string         `json:"schema"`
	Table         string         `json:"table"`
	Data          map[string]any `json:"data"`
	OldData       map[string]any `json:"old_data"`
}

type safeBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (buffer *safeBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.Write(data)
}

func (buffer *safeBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.String()
}

type testConsumer struct {
	command *exec.Cmd
	output  *safeBuffer
	done    chan struct{}

	mu      sync.Mutex
	exitErr error
	stopped bool
}

func startConsumer(t *testing.T, db *pgx.Conn, outputFile string, extraEnv map[string]string) *testConsumer {
	t.Helper()
	requireSlotInactive(t, db)

	overrides := map[string]string{
		"CDC_SLOT":            testSlotName,
		"CDC_SINK":            "file",
		"CDC_OUTPUT_FILE":     outputFile,
		"CDC_STATUS_INTERVAL": "50ms",
		"CDC_HEALTH_ENABLED":  "false",
	}
	for name, value := range extraEnv {
		overrides[name] = value
	}

	consumer := &testConsumer{
		command: exec.Command(consumerBinaryPath),
		output:  &safeBuffer{},
		done:    make(chan struct{}),
	}
	consumer.command.Env = environmentWith(overrides)
	consumer.command.Stdout = consumer.output
	consumer.command.Stderr = consumer.output
	if err := consumer.command.Start(); err != nil {
		t.Fatalf("iniciar consumidor: %v", err)
	}

	go func() {
		err := consumer.command.Wait()
		consumer.mu.Lock()
		consumer.exitErr = err
		consumer.mu.Unlock()
		close(consumer.done)
	}()

	t.Cleanup(func() {
		consumer.cleanup(db)
	})

	waitUntil(t, testTimeout, "consumidor iniciar streaming", func() (bool, error) {
		select {
		case <-consumer.done:
			return false, fmt.Errorf("consumidor encerrou durante a inicializacao: %v\n%s", consumer.error(), consumer.output.String())
		default:
		}
		return strings.Contains(consumer.output.String(), "aguardando eventos"), nil
	})

	return consumer
}

func (consumer *testConsumer) stop(t *testing.T, db *pgx.Conn, abrupt bool) {
	t.Helper()
	consumer.mu.Lock()
	if consumer.stopped {
		consumer.mu.Unlock()
		return
	}
	consumer.stopped = true
	consumer.mu.Unlock()

	var signalErr error
	if abrupt {
		signalErr = consumer.command.Process.Kill()
	} else {
		signalErr = consumer.command.Process.Signal(os.Interrupt)
	}
	if signalErr != nil && !errors.Is(signalErr, os.ErrProcessDone) {
		t.Fatalf("encerrar consumidor: %v", signalErr)
	}

	select {
	case <-consumer.done:
	case <-time.After(testTimeout):
		t.Fatalf("timeout encerrando consumidor:\n%s", consumer.output.String())
	}

	if !abrupt && consumer.error() != nil {
		t.Fatalf("consumidor encerrou com erro: %v\n%s", consumer.error(), consumer.output.String())
	}
	waitForSlotInactive(t, db)
}

func (consumer *testConsumer) cleanup(db *pgx.Conn) {
	consumer.mu.Lock()
	if consumer.stopped {
		consumer.mu.Unlock()
		return
	}
	consumer.stopped = true
	consumer.mu.Unlock()

	_ = consumer.command.Process.Kill()
	select {
	case <-consumer.done:
	case <-time.After(3 * time.Second):
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		state, err := querySlot(ctx, db)
		if err == nil && !state.active {
			return
		}
		time.Sleep(pollInterval)
	}
}

func (consumer *testConsumer) error() error {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	return consumer.exitErr
}

func openTestDatabase(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	config, err := sourcePGConfig()
	if err != nil {
		t.Fatalf("configurar PostgreSQL real: %v", err)
	}
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("conectar ao PostgreSQL real: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close(context.Background())
	})

	var plugin string
	err = conn.QueryRow(ctx, `
		SELECT plugin
		FROM pg_replication_slots
		WHERE slot_name = $1
		  AND slot_type = 'logical'
		  AND database = current_database()
	`, testSlotName).Scan(&plugin)
	if errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("slot %q nao existe; execute ./scripts/create-slot.sh", testSlotName)
	}
	if err != nil {
		t.Fatalf("consultar slot %q: %v", testSlotName, err)
	}
	if plugin != expectedPlugin {
		t.Fatalf("slot %q usa %q; esperado %q", testSlotName, plugin, expectedPlugin)
	}

	return conn
}

func readSlot(t *testing.T, db *pgx.Conn) databaseSlot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	state, err := querySlot(ctx, db)
	if err != nil {
		t.Fatalf("consultar estado do slot: %v", err)
	}
	return state
}

func querySlot(ctx context.Context, db *pgx.Conn) (databaseSlot, error) {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
	}

	var state databaseSlot
	var lsnText string
	err := db.QueryRow(ctx, `
		SELECT confirmed_flush_lsn::text, active
		FROM pg_replication_slots
		WHERE slot_name = $1
	`, testSlotName).Scan(&lsnText, &state.active)
	if err != nil {
		return state, err
	}
	state.confirmedLSN, err = pglogrepl.ParseLSN(lsnText)
	return state, err
}

func requireSlotInactive(t *testing.T, db *pgx.Conn) {
	t.Helper()
	if state := readSlot(t, db); state.active {
		t.Fatalf("slot %q esta ativo; encerre o consumidor antes dos testes", testSlotName)
	}
}

func waitForSlotInactive(t *testing.T, db *pgx.Conn) {
	t.Helper()
	waitUntil(t, testTimeout, "slot ficar inativo", func() (bool, error) {
		state, err := querySlot(context.Background(), db)
		return !state.active, err
	})
}

func waitForAcknowledgement(t *testing.T, db *pgx.Conn, target pglogrepl.LSN) {
	t.Helper()
	waitUntil(t, testTimeout, fmt.Sprintf("confirmacao do LSN %s", target), func() (bool, error) {
		state, err := querySlot(context.Background(), db)
		return state.confirmedLSN >= target, err
	})
}

func waitFileContains(t *testing.T, filename string, values ...string) {
	t.Helper()
	waitUntil(t, testTimeout, fmt.Sprintf("arquivo %s conter %v", filename, values), func() (bool, error) {
		content, err := os.ReadFile(filename)
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		for _, value := range values {
			if !bytes.Contains(content, []byte(value)) {
				return false, nil
			}
		}
		return true, nil
	})
}

func waitConsumerOutput(t *testing.T, consumer *testConsumer, value string) {
	t.Helper()
	waitUntil(t, testTimeout, fmt.Sprintf("stdout conter %q", value), func() (bool, error) {
		select {
		case <-consumer.done:
			return false, fmt.Errorf("consumidor encerrou: %v\n%s", consumer.error(), consumer.output.String())
		default:
		}
		return strings.Contains(consumer.output.String(), value), nil
	})
}

func waitConsumerDone(t *testing.T, consumer *testConsumer) {
	t.Helper()
	select {
	case <-consumer.done:
	case <-time.After(testTimeout):
		t.Fatalf("timeout aguardando consumidor encerrar:\n%s", consumer.output.String())
	}
	consumer.mu.Lock()
	consumer.stopped = true
	consumer.mu.Unlock()
}

func waitUntil(t *testing.T, timeout time.Duration, description string, condition func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ready, err := condition()
		if err != nil {
			t.Fatalf("%s: %v", description, err)
		}
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout aguardando %s", description)
		}
		time.Sleep(pollInterval)
	}
}

func execSQL(t *testing.T, db *pgx.Conn, sql string, arguments ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := db.Exec(ctx, sql, arguments...); err != nil {
		t.Fatalf("executar SQL de teste: %v", err)
	}
}

func execTransaction(t *testing.T, db *pgx.Conn, firstSQL string, firstArgs []any, secondSQL string, secondArgs []any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("iniciar transacao de teste: %v", err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, firstSQL, firstArgs...); err != nil {
		t.Fatalf("primeira operacao da transacao: %v", err)
	}
	if _, err := tx.Exec(ctx, secondSQL, secondArgs...); err != nil {
		t.Fatalf("segunda operacao da transacao: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit da transacao de teste: %v", err)
	}
}

func execInsertUpdateDeleteTransaction(t *testing.T, db *pgx.Conn, name, updatedName, email string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("iniciar transacao de protocolo: %v", err)
	}
	defer tx.Rollback(context.Background())

	var xid int64
	if err := tx.QueryRow(ctx, "SELECT txid_current()").Scan(&xid); err != nil {
		t.Fatalf("consultar xid da transacao: %v", err)
	}
	var id int64
	if err := tx.QueryRow(ctx,
		"INSERT INTO clientes (nome, email) VALUES ($1, $2) RETURNING id",
		name, email,
	).Scan(&id); err != nil {
		t.Fatalf("INSERT da transacao de protocolo: %v", err)
	}
	if _, err := tx.Exec(ctx, "UPDATE clientes SET nome = $1 WHERE id = $2", updatedName, id); err != nil {
		t.Fatalf("UPDATE da transacao de protocolo: %v", err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM clientes WHERE id = $1", id); err != nil {
		t.Fatalf("DELETE da transacao de protocolo: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit da transacao de protocolo: %v", err)
	}
	return uint64(xid)
}

func execRollbackTransaction(t *testing.T, db *pgx.Conn, sql string, arguments ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("iniciar transacao de rollback: %v", err)
	}
	if _, err := tx.Exec(ctx, sql, arguments...); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatalf("executar operacao de rollback: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback da transacao de teste: %v", err)
	}
}

func cleanupEmail(t *testing.T, db *pgx.Conn, email string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := db.Exec(ctx, "DELETE FROM clientes WHERE email = $1", email); err != nil {
			t.Logf("limpeza do registro %q falhou: %v", email, err)
		}
	})
}

func cloneDestinationConfig(t *testing.T, db *pgx.Conn) *pgx.ConnConfig {
	t.Helper()
	config := db.Config().Copy()
	delete(config.RuntimeParams, "replication")
	return config
}

func assertClienteName(t *testing.T, db *pgx.Conn, email, expected string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var name string
	if err := db.QueryRow(ctx, "SELECT nome FROM clientes WHERE email = $1", email).Scan(&name); err != nil {
		t.Fatalf("consultar cliente %q: %v", email, err)
	}
	if name != expected {
		t.Fatalf("nome inesperado para %q: esperado=%q atual=%q", email, expected, name)
	}
}

func assertClienteMissing(t *testing.T, db *pgx.Conn, email string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var exists bool
	if err := db.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM clientes WHERE email = $1)", email).Scan(&exists); err != nil {
		t.Fatalf("consultar existencia de %q: %v", email, err)
	}
	if exists {
		t.Fatalf("cliente %q nao deveria existir", email)
	}
}

func testIdentity(prefix string) (string, string) {
	name := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	return name, strings.ToLower(name) + "@teste.com"
}

func eventLSN(t *testing.T, filename, identity string) pglogrepl.LSN {
	t.Helper()
	for _, event := range readEvents(t, filename) {
		if eventContains(event, identity) {
			lsn, err := pglogrepl.ParseLSN(event.LSN)
			if err != nil {
				t.Fatalf("LSN invalido no evento %+v: %v", event, err)
			}
			return lsn
		}
	}
	t.Fatalf("evento %q nao encontrado no conteudo:\n%s", identity, readOutputFile(t, filename))
	return 0
}

func waitForEvent(t *testing.T, filename, identity string, eventType eventType) recordedEvent {
	t.Helper()
	var foundEvent recordedEvent
	waitUntil(t, testTimeout, fmt.Sprintf("evento %s contendo %q", eventType, identity), func() (bool, error) {
		for _, event := range readEventsIfExists(t, filename) {
			if event.Type == eventType && eventContains(event, identity) {
				foundEvent = event
				return true, nil
			}
		}
		return false, nil
	})
	return foundEvent
}

func assertCommonEvent(t *testing.T, event recordedEvent) {
	t.Helper()
	if event.TransactionID == 0 {
		t.Fatalf("evento sem transaction_id: %+v", event)
	}
	if event.Schema != "public" || event.Table != "clientes" {
		t.Fatalf("evento com relacao inesperada: %+v", event)
	}
	if _, err := pglogrepl.ParseLSN(event.LSN); err != nil {
		t.Fatalf("evento com LSN invalido: %+v err=%v", event, err)
	}
}

func readOutputFile(t *testing.T, filename string) string {
	t.Helper()
	content, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("ler %s: %v", filename, err)
	}
	return string(content)
}

func readOutputFileIfExists(t *testing.T, filename string) string {
	t.Helper()
	content, err := os.ReadFile(filename)
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	if err != nil {
		t.Fatalf("ler %s: %v", filename, err)
	}
	return string(content)
}

func readEvents(t *testing.T, filename string) []recordedEvent {
	t.Helper()
	return parseEvents(t, readOutputFile(t, filename))
}

func readEventsIfExists(t *testing.T, filename string) []recordedEvent {
	t.Helper()
	content := readOutputFileIfExists(t, filename)
	if content == "" {
		return nil
	}
	return parseEvents(t, content)
}

func parseEvents(t *testing.T, content string) []recordedEvent {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(content), "\n")
	events := make([]recordedEvent, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var event recordedEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("linha nao e JSON valido: %q err=%v", line, err)
		}
		events = append(events, event)
	}
	return events
}

func eventIndex(events []recordedEvent, value string) int {
	for index, event := range events {
		if eventContains(event, value) {
			return index
		}
	}
	return -1
}

func eventContains(event recordedEvent, value string) bool {
	return mapContains(event.Data, value) || mapContains(event.OldData, value)
}

func mapContains(values map[string]any, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func environmentWith(overrides map[string]string) []string {
	environment := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if _, overridden := overrides[name]; !overridden {
			environment = append(environment, entry)
		}
	}
	for name, value := range overrides {
		environment = append(environment, name+"="+value)
	}
	return environment
}
