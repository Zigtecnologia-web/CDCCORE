package main

import (
	"fmt"
	"strings"
)

type qualifiedTable struct {
	schema string
	name   string
}

func (table qualifiedTable) key() string {
	return table.schema + "." + table.name
}

func parseTableList(value string) ([]qualifiedTable, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}

	parts := strings.Split(value, ",")
	tables := make([]qualifiedTable, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		segments := strings.Split(name, ".")
		if len(segments) != 2 || segments[0] == "" || segments[1] == "" {
			return nil, fmt.Errorf("CDC_TABLE_INCLUDE deve usar schema.tabela: %q", name)
		}
		table := qualifiedTable{schema: segments[0], name: segments[1]}
		if _, exists := seen[table.key()]; exists {
			continue
		}
		seen[table.key()] = struct{}{}
		tables = append(tables, table)
	}
	return tables, nil
}
