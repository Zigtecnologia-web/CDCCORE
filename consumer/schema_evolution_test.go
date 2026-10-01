package main

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestRelationMetadataComparison(t *testing.T) {
	base := RelationMetadata{
		RelationID: 7,
		Schema:     "public",
		Table:      "clientes",
		Columns: []RelationColumn{
			{Name: "id", TypeOID: pgtype.Int8OID, TypeMod: -1},
			{Name: "nome", TypeOID: pgtype.TextOID, TypeMod: -1},
		},
	}
	tests := []struct {
		name string
		next RelationMetadata
		want schemaMetadataClassification
	}{
		{name: "identica", next: base, want: ""},
		{name: "coluna adicionada", next: withColumns(base, append(cloneRelationColumns(base.Columns), RelationColumn{Name: "email", TypeOID: pgtype.TextOID, TypeMod: -1})), want: metadataColumnAdded},
		{name: "coluna removida", next: withColumns(base, cloneRelationColumns(base.Columns[:1])), want: metadataColumnRemoved},
		{name: "oid alterado", next: withColumns(base, []RelationColumn{{Name: "id", TypeOID: pgtype.Int8OID, TypeMod: -1}, {Name: "nome", TypeOID: pgtype.VarcharOID, TypeMod: -1}}), want: metadataColumnChanged},
		{name: "typmod alterado", next: withColumns(base, []RelationColumn{{Name: "id", TypeOID: pgtype.Int8OID, TypeMod: -1}, {Name: "nome", TypeOID: pgtype.TextOID, TypeMod: 20}}), want: metadataColumnChanged},
		{name: "metadata vazia", next: withColumns(base, nil), want: metadataColumnRemoved},
		{name: "rename ambiguo", next: withColumns(base, []RelationColumn{{Name: "id", TypeOID: pgtype.Int8OID, TypeMod: -1}, {Name: "nome_completo", TypeOID: pgtype.TextOID, TypeMod: -1}}), want: metadataUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changes := compareRelationMetadata(base, test.next, 0x100, 10)
			if test.want == "" {
				if len(changes) != 0 {
					t.Fatalf("metadata identica gerou mudancas: %+v", changes)
				}
				return
			}
			if !hasMetadataClassification(changes, test.want) {
				t.Fatalf("classificacao %s ausente em %+v", test.want, changes)
			}
		})
	}
}

func TestSchemaStateRelationIdentityAndConsecutiveSnapshots(t *testing.T) {
	state := schemaState{}
	baseline := relationMessage(7, "clientes", "id")
	if changes, err := state.observeRelation(baseline, 0x100, 1); err != nil || len(changes) != 0 {
		t.Fatalf("baseline: changes=%+v err=%v", changes, err)
	}

	relationIDChanged := relationMessage(8, "clientes", "id")
	changes, err := state.observeRelation(relationIDChanged, 0x110, 2)
	if err != nil || !hasMetadataClassification(changes, metadataRelationIDChanged) {
		t.Fatalf("relation id alterado: changes=%+v err=%v", changes, err)
	}

	renamed := relationMessage(8, "clientes_novos", "id")
	changes, err = state.observeRelation(renamed, 0x120, 3)
	if err != nil || !hasMetadataClassification(changes, metadataRelationRenamedOrReplaced) {
		t.Fatalf("relacao renomeada/substituida: changes=%+v err=%v", changes, err)
	}

	added := relationMessage(8, "clientes_novos", "id", "email")
	changes, err = state.observeRelation(added, 0x130, 4)
	if err != nil || !hasMetadataClassification(changes, metadataColumnAdded) {
		t.Fatalf("snapshot consecutivo: changes=%+v err=%v", changes, err)
	}
	if changes, err = state.observeRelation(added, 0x140, 5); err != nil || len(changes) != 0 {
		t.Fatalf("snapshot repetido: changes=%+v err=%v", changes, err)
	}
}

func TestSchemaStateObservesCollapsedConsecutiveAdds(t *testing.T) {
	state := schemaState{}
	if changes, err := state.observeRelation(relationMessage(7, "clientes", "id"), 0x100, 1); err != nil || len(changes) != 0 {
		t.Fatalf("baseline: changes=%+v err=%v", changes, err)
	}

	changes, err := state.observeRelation(relationMessage(7, "clientes", "id", "a", "b"), 0x140, 2)
	if err != nil {
		t.Fatalf("snapshot colapsado: %v", err)
	}
	if len(changes) != 2 {
		t.Fatalf("esperadas duas diferencas observaveis, recebido %+v", changes)
	}
	for _, change := range changes {
		if change.Classification != metadataColumnAdded {
			t.Fatalf("snapshot A->C nao deveria inferir operacao intermediaria: %+v", changes)
		}
	}
}

func TestSchemaStateDoesNotReplaceSnapshotWithInvalidMetadata(t *testing.T) {
	state := schemaState{}
	baseline := relationMessage(7, "clientes", "id")
	if _, err := state.observeRelation(baseline, 0x100, 1); err != nil {
		t.Fatalf("baseline: %v", err)
	}
	invalid := relationMessage(7, "clientes", "id", "id")
	if _, err := state.observeRelation(invalid, 0x110, 2); err == nil {
		t.Fatal("metadata duplicada deveria ser invalida")
	}
	changes, err := state.observeRelation(relationMessage(7, "clientes", "id", "email"), 0x120, 3)
	if err != nil || !hasMetadataClassification(changes, metadataColumnAdded) {
		t.Fatalf("snapshot valido anterior nao foi preservado: changes=%+v err=%v", changes, err)
	}
}

func TestSchemaApplyCandidatesFromValidatedDestinationDifference(t *testing.T) {
	column := columnSchema{Name: "telefone", DataType: "text", TypeOID: pgtype.TextOID, Typmod: -1, Nullable: true}
	validation := schemaValidationResult{
		Status:        compatibilityIncompatible,
		Schema:        "public",
		Table:         "clientes",
		LSN:           0x200,
		TransactionID: 20,
		SourceSchema:  tableSchema{Columns: []columnSchema{column}},
		Differences:   []schemaDifference{{Type: differenceColumnMissing, Column: column.Name}},
	}
	changes := schemaApplyCandidates(nil, validation, 7)
	if len(changes) != 1 || changes[0].Classification != metadataColumnAdded || !changes[0].HasColumn {
		t.Fatalf("candidato de recovery inesperado: %+v", changes)
	}
	ambiguous := []schemaChange{{Classification: metadataUnknown, Details: map[string]any{"reason": "possible_rename"}}}
	changes = schemaApplyCandidates(ambiguous, validation, 7)
	if len(changes) != 1 || changes[0].Classification != metadataUnknown {
		t.Fatalf("metadata ambigua nao deveria gerar candidato de apply: %+v", changes)
	}
}

func TestSchemaMetadataMetricsSeparateUnknownAndRelationMessages(t *testing.T) {
	metrics := newMetrics()
	metrics.schemaMetadataRelationMessages.Add(2)
	logSchemaChanges("unit-test", []schemaChange{
		{Classification: metadataColumnAdded, Details: map[string]any{"column": "email"}},
		{Classification: metadataUnknown, Details: map[string]any{"reason": "ambiguous"}},
	}, metrics)
	if got := metrics.schemaMetadataChanges.Load(); got != 2 {
		t.Fatalf("metadata changes=%d, esperado 2", got)
	}
	if got := metrics.schemaMetadataUnknown.Load(); got != 1 {
		t.Fatalf("metadata unknown=%d, esperado 1", got)
	}
	if got := metrics.snapshot()["cdc_schema_metadata_relation_messages_total"]; got != uint64(2) {
		t.Fatalf("relation messages=%v, esperado 2", got)
	}
}

func TestSchemaEvolutionAutoRequiresGenericPostgresSink(t *testing.T) {
	connection, err := pgx.ParseConfig("postgres://postgres:postgres@localhost:5432/cdc_demo")
	if err != nil {
		t.Fatalf("config PostgreSQL de teste: %v", err)
	}
	cfg := appConfig{
		sourceID:          "source",
		slotName:          "slot",
		publication:       "publication",
		sourceConfig:      connection,
		destConfig:        connection.Copy(),
		sinkName:          "postgres",
		postgresApplyMode: "explicit",
		schemaEvolution:   "auto",
	}
	if err := validateConfig(cfg); err == nil {
		t.Fatal("schema evolution auto com apply explicit deveria ser rejeitado")
	}
	cfg.postgresApplyMode = "generic"
	cfg.includedTables = []qualifiedTable{{schema: "public", name: "clientes"}}
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("schema evolution auto com apply generic deveria ser valido: %v", err)
	}
}

func relationMessage(id uint32, table string, columns ...string) *pglogrepl.RelationMessage {
	message := &pglogrepl.RelationMessage{RelationID: id, Namespace: "public", RelationName: table}
	for _, name := range columns {
		message.Columns = append(message.Columns, &pglogrepl.RelationMessageColumn{Name: name, DataType: pgtype.TextOID, TypeModifier: -1})
	}
	return message
}

func withColumns(metadata RelationMetadata, columns []RelationColumn) RelationMetadata {
	metadata.Columns = columns
	return metadata
}

func cloneRelationColumns(columns []RelationColumn) []RelationColumn {
	return append([]RelationColumn(nil), columns...)
}

func hasMetadataClassification(changes []schemaChange, classification schemaMetadataClassification) bool {
	for _, change := range changes {
		if change.Classification == classification {
			return true
		}
	}
	return false
}
