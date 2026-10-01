package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/jackc/pglogrepl"
)

type schemaAuditLogger struct {
	catalog *schemaCatalogReader
}

type schemaAuditMessage struct {
	Event             string              `json:"event"`
	MessageType       string              `json:"message_type"`
	WALStart          string              `json:"wal_start,omitempty"`
	ServerWALEnd      string              `json:"server_wal_end,omitempty"`
	LSN               string              `json:"lsn,omitempty"`
	TransactionID     uint32              `json:"transaction_id,omitempty"`
	RelationID        uint32              `json:"relation_id,omitempty"`
	Schema            string              `json:"schema,omitempty"`
	Table             string              `json:"table,omitempty"`
	ReplicaIdentity   uint8               `json:"replica_identity,omitempty"`
	Columns           []schemaAuditColumn `json:"columns,omitempty"`
	CatalogColumns    []schemaAuditColumn `json:"catalog_columns,omitempty"`
	CatalogReadStatus string              `json:"catalog_read_status,omitempty"`
	Details           map[string]any      `json:"details,omitempty"`
}

type schemaAuditColumn struct {
	Name         string `json:"name"`
	DataTypeOID  uint32 `json:"data_type_oid,omitempty"`
	DataType     string `json:"data_type,omitempty"`
	TypeModifier int32  `json:"type_modifier,omitempty"`
	Key          bool   `json:"key,omitempty"`
	Nullable     bool   `json:"nullable,omitempty"`
	HasDefault   bool   `json:"has_default,omitempty"`
}

func (logger *schemaAuditLogger) logMessage(
	ctx context.Context,
	message pglogrepl.Message,
	walStart pglogrepl.LSN,
	serverWALEnd pglogrepl.LSN,
	transactionID uint32,
) {
	audit := schemaAuditMessage{
		Event:         "schema_evolution_audit",
		MessageType:   auditMessageType(message),
		WALStart:      walStart.String(),
		ServerWALEnd:  serverWALEnd.String(),
		LSN:           walStart.String(),
		TransactionID: transactionID,
	}

	switch message := message.(type) {
	case *pglogrepl.BeginMessage:
		audit.LSN = message.FinalLSN.String()
		audit.TransactionID = message.Xid
		audit.Details = map[string]any{"commit_time": message.CommitTime}
	case *pglogrepl.RelationMessage:
		audit.RelationID = message.RelationID
		audit.Schema = message.Namespace
		audit.Table = message.RelationName
		audit.ReplicaIdentity = message.ReplicaIdentity
		audit.Columns = auditRelationColumns(message)
		logger.addCatalogSnapshot(ctx, &audit)
	case *pglogrepl.InsertMessage:
		audit.RelationID = message.RelationID
		audit.Details = map[string]any{"tuple_columns": len(message.Tuple.Columns)}
	case *pglogrepl.UpdateMessage:
		audit.RelationID = message.RelationID
		details := map[string]any{"new_tuple_columns": len(message.NewTuple.Columns)}
		if message.OldTuple != nil {
			details["old_tuple_columns"] = len(message.OldTuple.Columns)
		}
		audit.Details = details
	case *pglogrepl.DeleteMessage:
		audit.RelationID = message.RelationID
		if message.OldTuple != nil {
			audit.Details = map[string]any{"old_tuple_columns": len(message.OldTuple.Columns)}
		}
	case *pglogrepl.CommitMessage:
		audit.LSN = message.TransactionEndLSN.String()
		audit.Details = map[string]any{
			"commit_lsn":          message.CommitLSN.String(),
			"transaction_end_lsn": message.TransactionEndLSN.String(),
			"commit_time":         message.CommitTime,
		}
	}

	payload, err := json.Marshal(audit)
	if err != nil {
		log.Printf("level=warn event=schema_evolution_audit_error error=%q", err.Error())
		return
	}
	log.Printf("level=info %s", payload)
}

func (logger *schemaAuditLogger) addCatalogSnapshot(ctx context.Context, audit *schemaAuditMessage) {
	if logger.catalog == nil || audit.Schema == "" || audit.Table == "" {
		return
	}
	schema, exists, err := logger.catalog.ReadTableSchema(ctx, qualifiedTable{schema: audit.Schema, name: audit.Table})
	if err != nil {
		audit.CatalogReadStatus = "ERROR: " + err.Error()
		return
	}
	if !exists {
		audit.CatalogReadStatus = "TABLE_NOT_FOUND"
		return
	}
	audit.CatalogReadStatus = "OK"
	audit.CatalogColumns = make([]schemaAuditColumn, 0, len(schema.Columns))
	for _, column := range schema.Columns {
		audit.CatalogColumns = append(audit.CatalogColumns, schemaAuditColumn{
			Name:         column.Name,
			DataType:     column.DataType,
			DataTypeOID:  column.TypeOID,
			TypeModifier: column.Typmod,
			Nullable:     column.Nullable,
			HasDefault:   column.HasDefault,
		})
	}
}

func auditMessageType(message pglogrepl.Message) string {
	switch message.(type) {
	case *pglogrepl.BeginMessage:
		return "BEGIN"
	case *pglogrepl.CommitMessage:
		return "COMMIT"
	case *pglogrepl.RelationMessage:
		return "RELATION"
	case *pglogrepl.InsertMessage:
		return "INSERT"
	case *pglogrepl.UpdateMessage:
		return "UPDATE"
	case *pglogrepl.DeleteMessage:
		return "DELETE"
	case *pglogrepl.TypeMessage:
		return "TYPE"
	case *pglogrepl.OriginMessage:
		return "ORIGIN"
	default:
		return "UNKNOWN"
	}
}

func auditRelationColumns(message *pglogrepl.RelationMessage) []schemaAuditColumn {
	columns := make([]schemaAuditColumn, 0, len(message.Columns))
	for _, column := range message.Columns {
		columns = append(columns, schemaAuditColumn{
			Name:         column.Name,
			DataTypeOID:  column.DataType,
			TypeModifier: column.TypeModifier,
			Key:          column.Flags&1 == 1,
		})
	}
	return columns
}
