package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/jackc/pglogrepl"
)

type schemaMetadataClassification string

const (
	metadataColumnAdded               schemaMetadataClassification = "COLUMN_ADDED"
	metadataColumnRemoved             schemaMetadataClassification = "COLUMN_REMOVED"
	metadataColumnChanged             schemaMetadataClassification = "COLUMN_METADATA_CHANGED"
	metadataRelationIDChanged         schemaMetadataClassification = "RELATION_ID_CHANGED"
	metadataRelationRenamedOrReplaced schemaMetadataClassification = "RELATION_RENAMED_OR_REPLACED"
	metadataUnknown                   schemaMetadataClassification = "UNKNOWN"
)

// RelationMetadata contains only fields supplied by pgoutput RelationMessage.
// Catalog-only properties such as nullability and defaults live in columnSchema.
type RelationMetadata struct {
	RelationID uint32
	Schema     string
	Table      string
	Columns    []RelationColumn
}

type RelationColumn struct {
	Name    string
	TypeOID uint32
	TypeMod int32
}

// schemaChange is an internal metadata observation. It is deliberately
// separate from both the public DML event contract and an inferred DDL command.
type schemaChange struct {
	LSN            pglogrepl.LSN
	TransactionID  uint32
	Schema         string
	Table          string
	RelationID     uint32
	Classification schemaMetadataClassification
	Column         columnSchema
	HasColumn      bool
	Details        map[string]any
}

type schemaState struct {
	byRelationID map[uint32]RelationMetadata
	byName       map[string]uint32
}

func relationMetadata(message *pglogrepl.RelationMessage) (RelationMetadata, error) {
	if message == nil {
		return RelationMetadata{}, fmt.Errorf("RelationMessage ausente")
	}
	if message.RelationID == 0 {
		return RelationMetadata{}, fmt.Errorf("RelationMessage sem relation_id")
	}
	if message.Namespace == "" || message.RelationName == "" {
		return RelationMetadata{}, fmt.Errorf("RelationMessage relation_id=%d sem schema ou tabela", message.RelationID)
	}

	metadata := RelationMetadata{
		RelationID: message.RelationID,
		Schema:     message.Namespace,
		Table:      message.RelationName,
		Columns:    make([]RelationColumn, 0, len(message.Columns)),
	}
	seen := make(map[string]struct{}, len(message.Columns))
	for index, column := range message.Columns {
		if column == nil {
			return RelationMetadata{}, fmt.Errorf("RelationMessage relation_id=%d possui coluna %d ausente", message.RelationID, index)
		}
		if column.Name == "" {
			return RelationMetadata{}, fmt.Errorf("RelationMessage relation_id=%d possui coluna %d sem nome", message.RelationID, index)
		}
		if _, exists := seen[column.Name]; exists {
			return RelationMetadata{}, fmt.Errorf("RelationMessage relation_id=%d possui coluna duplicada %q", message.RelationID, column.Name)
		}
		seen[column.Name] = struct{}{}
		metadata.Columns = append(metadata.Columns, RelationColumn{
			Name:    column.Name,
			TypeOID: column.DataType,
			TypeMod: column.TypeModifier,
		})
	}
	return metadata, nil
}

func (metadata RelationMetadata) key() string {
	return qualifiedTable{schema: metadata.Schema, name: metadata.Table}.key()
}

func (state *schemaState) observeRelation(message *pglogrepl.RelationMessage, lsn pglogrepl.LSN, xid uint32) ([]schemaChange, error) {
	next, err := relationMetadata(message)
	if err != nil {
		return nil, err
	}
	if state.byRelationID == nil {
		state.byRelationID = make(map[uint32]RelationMetadata)
		state.byName = make(map[string]uint32)
	}

	previous, existsByID := state.byRelationID[next.RelationID]
	previousID, existsByName := state.byName[next.key()]
	changes := make([]schemaChange, 0)
	if existsByID {
		changes = append(changes, compareRelationMetadata(previous, next, lsn, xid)...)
	} else if existsByName && previousID != next.RelationID {
		previous = state.byRelationID[previousID]
		changes = append(changes, newMetadataChange(next, lsn, xid, metadataRelationIDChanged, map[string]any{
			"previous_relation_id": previousID,
			"current_relation_id":  next.RelationID,
		}))
		changes = append(changes, compareRelationColumns(previous, next, lsn, xid)...)
	}

	// Replace the snapshot only after the RelationMessage was fully validated
	// and compared.
	if existsByID && previous.key() != next.key() {
		delete(state.byName, previous.key())
	}
	if existsByName && previousID != next.RelationID {
		delete(state.byRelationID, previousID)
	}
	state.byRelationID[next.RelationID] = next
	state.byName[next.key()] = next.RelationID
	return changes, nil
}

func compareRelationMetadata(previous, next RelationMetadata, lsn pglogrepl.LSN, xid uint32) []schemaChange {
	changes := make([]schemaChange, 0)
	if previous.Schema != next.Schema || previous.Table != next.Table {
		changes = append(changes, newMetadataChange(next, lsn, xid, metadataRelationRenamedOrReplaced, map[string]any{
			"previous_schema": previous.Schema,
			"previous_table":  previous.Table,
			"current_schema":  next.Schema,
			"current_table":   next.Table,
		}))
	}
	changes = append(changes, compareRelationColumns(previous, next, lsn, xid)...)
	return changes
}

func compareRelationColumns(previous, next RelationMetadata, lsn pglogrepl.LSN, xid uint32) []schemaChange {
	changes := make([]schemaChange, 0)
	previousByName := make(map[string]RelationColumn, len(previous.Columns))
	nextByName := make(map[string]RelationColumn, len(next.Columns))
	for _, column := range previous.Columns {
		previousByName[column.Name] = column
	}
	for _, column := range next.Columns {
		nextByName[column.Name] = column
	}

	added := make([]RelationColumn, 0)
	removed := make([]RelationColumn, 0)
	for _, column := range next.Columns {
		if _, exists := previousByName[column.Name]; !exists {
			added = append(added, column)
		}
	}
	for _, column := range previous.Columns {
		if _, exists := nextByName[column.Name]; !exists {
			removed = append(removed, column)
		}
	}

	// A simultaneous removal and addition can be a rename, replacement, or
	// multiple operations. pgoutput does not provide enough evidence to decide.
	if len(added) > 0 && len(removed) > 0 {
		changes = append(changes, newMetadataChange(next, lsn, xid, metadataUnknown, map[string]any{
			"reason":          "columns_added_and_removed",
			"added_columns":   relationColumnNames(added),
			"removed_columns": relationColumnNames(removed),
		}))
	} else {
		for _, column := range added {
			changes = append(changes, newMetadataChange(next, lsn, xid, metadataColumnAdded, map[string]any{
				"column":        column.Name,
				"data_type_oid": column.TypeOID,
				"type_modifier": column.TypeMod,
			}))
		}
		for _, column := range removed {
			changes = append(changes, newMetadataChange(next, lsn, xid, metadataColumnRemoved, map[string]any{
				"column":        column.Name,
				"data_type_oid": column.TypeOID,
				"type_modifier": column.TypeMod,
			}))
		}
	}

	for _, column := range next.Columns {
		previousColumn, exists := previousByName[column.Name]
		if !exists || previousColumn == column {
			continue
		}
		changes = append(changes, newMetadataChange(next, lsn, xid, metadataColumnChanged, map[string]any{
			"column":                 column.Name,
			"previous_data_type_oid": previousColumn.TypeOID,
			"previous_type_modifier": previousColumn.TypeMod,
			"current_data_type_oid":  column.TypeOID,
			"current_type_modifier":  column.TypeMod,
		}))
	}

	if sameRelationColumnSet(previous.Columns, next.Columns) && !sameRelationColumnOrder(previous.Columns, next.Columns) {
		changes = append(changes, newMetadataChange(next, lsn, xid, metadataUnknown, map[string]any{
			"reason":         "column_order_changed",
			"previous_order": relationColumnNames(previous.Columns),
			"current_order":  relationColumnNames(next.Columns),
		}))
	}
	return changes
}

func newMetadataChange(metadata RelationMetadata, lsn pglogrepl.LSN, xid uint32, classification schemaMetadataClassification, details map[string]any) schemaChange {
	return schemaChange{
		LSN:            lsn,
		TransactionID:  xid,
		Schema:         metadata.Schema,
		Table:          metadata.Table,
		RelationID:     metadata.RelationID,
		Classification: classification,
		Details:        details,
	}
}

func sameRelationColumnSet(left, right []RelationColumn) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]struct{}, len(left))
	for _, column := range left {
		seen[column.Name] = struct{}{}
	}
	for _, column := range right {
		if _, exists := seen[column.Name]; !exists {
			return false
		}
	}
	return true
}

func sameRelationColumnOrder(left, right []RelationColumn) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Name != right[index].Name {
			return false
		}
	}
	return true
}

func relationColumnNames(columns []RelationColumn) []string {
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name)
	}
	return names
}

func enrichSchemaChanges(changes []schemaChange, validation schemaValidationResult) []schemaChange {
	if len(changes) == 0 || len(validation.SourceSchema.Columns) == 0 {
		return changes
	}
	if !metadataChangesAllowColumnApply(changes) {
		return changes
	}
	sourceColumns := columnsByName(validation.SourceSchema.Columns)
	for index := range changes {
		if changes[index].Classification != metadataColumnAdded {
			continue
		}
		columnName, _ := changes[index].Details["column"].(string)
		if column, exists := sourceColumns[columnName]; exists {
			changes[index].Column = column
			changes[index].HasColumn = true
		}
	}
	return changes
}

// schemaApplyCandidates turns a validated source/destination mismatch into an
// internal apply candidate. This does not claim that the source emitted an ADD
// COLUMN DDL; it describes the only destination action currently supported.
func schemaApplyCandidates(changes []schemaChange, validation schemaValidationResult, relationID uint32) []schemaChange {
	if validation.Status != compatibilityIncompatible {
		return changes
	}
	if len(changes) > 0 && !metadataChangesAllowColumnApply(changes) {
		return changes
	}
	existing := make(map[string]struct{})
	for _, change := range changes {
		if change.Classification != metadataColumnAdded {
			continue
		}
		if name, ok := change.Details["column"].(string); ok {
			existing[name] = struct{}{}
		}
	}
	sourceColumns := columnsByName(validation.SourceSchema.Columns)
	for _, difference := range validation.Differences {
		if difference.Type != differenceColumnMissing {
			continue
		}
		if _, exists := existing[difference.Column]; exists {
			continue
		}
		column, exists := sourceColumns[difference.Column]
		if !exists {
			continue
		}
		changes = append(changes, schemaChange{
			LSN:            validation.LSN,
			TransactionID:  validation.TransactionID,
			Schema:         validation.Schema,
			Table:          validation.Table,
			RelationID:     relationID,
			Classification: metadataColumnAdded,
			Column:         column,
			HasColumn:      true,
			Details: map[string]any{
				"column": column.Name,
				"basis":  "validated_destination_column_missing",
			},
		})
		existing[column.Name] = struct{}{}
	}
	return changes
}

func metadataChangesAllowColumnApply(changes []schemaChange) bool {
	for _, change := range changes {
		if change.Classification != metadataColumnAdded {
			return false
		}
	}
	return true
}

func logSchemaChanges(sourceID string, changes []schemaChange, metrics *metrics) {
	for _, change := range changes {
		details, err := json.Marshal(change.Details)
		if err != nil {
			details = []byte(`{"serialization_error":true}`)
		}
		column, _ := change.Details["column"].(string)
		reason := schemaMetadataChangeReason(change)
		if metrics != nil {
			metrics.schemaMetadataChanges.Add(1)
			if change.Classification == metadataUnknown {
				metrics.schemaMetadataUnknown.Add(1)
			}
		}
		log.Printf(
			"level=info event=schema_metadata_change_observed source_id=%s transaction_id=%d lsn=%s schema=%s table=%s relation_id=%d classification=%s reason=%s column=%s details=%s",
			sourceID,
			change.TransactionID,
			change.LSN,
			change.Schema,
			change.Table,
			change.RelationID,
			change.Classification,
			reason,
			column,
			details,
		)
	}
}

func schemaMetadataChangeReason(change schemaChange) string {
	if reason, ok := change.Details["reason"].(string); ok && reason != "" {
		return reason
	}
	switch change.Classification {
	case metadataColumnAdded:
		return "column_added_observed"
	case metadataColumnRemoved:
		return "column_removed_observed"
	case metadataColumnChanged:
		return "column_metadata_changed_observed"
	case metadataRelationIDChanged:
		return "relation_id_changed_observed"
	case metadataRelationRenamedOrReplaced:
		return "relation_name_changed_observed"
	case metadataUnknown:
		return "insufficient_relation_metadata"
	default:
		return "relation_metadata_changed_observed"
	}
}
