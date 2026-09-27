package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	defaultSourceID          = "cdc-demo"
	defaultSink              = "file"
	defaultPostgresApplyMode = "explicit"
	defaultTableInclude      = "public.clientes,public.enderecos"
	defaultRetryInitialDelay = time.Second
	defaultRetryMaxDelay     = 30 * time.Second
	defaultHealthAddr        = ":8080"
)

type appConfig struct {
	sourceID                 string
	slotName                 string
	publication              string
	outputFileName           string
	statusInterval           time.Duration
	sourceConfig             *pgx.ConnConfig
	destConfig               *pgx.ConnConfig
	sinkName                 string
	postgresApplyMode        string
	includedTables           []qualifiedTable
	publicationAutoConfigure bool
	retry                    retryConfig
	healthAddr               string
	enableHealth             bool
	pauseBeforeAck           bool
	pauseMatch               string
}

type retryConfig struct {
	initialDelay time.Duration
	maxDelay     time.Duration
	maxAttempts  int
}

func loadConfig() (appConfig, error) {
	statusInterval, err := durationFromEnv("CDC_STATUS_INTERVAL", defaultStatusInterval)
	if err != nil {
		return appConfig{}, err
	}
	initialDelay, err := durationFromEnv("CDC_RETRY_INITIAL_DELAY", defaultRetryInitialDelay)
	if err != nil {
		return appConfig{}, err
	}
	maxDelay, err := durationFromEnv("CDC_RETRY_MAX_DELAY", defaultRetryMaxDelay)
	if err != nil {
		return appConfig{}, err
	}
	maxAttempts, err := intFromEnv("CDC_RETRY_MAX_ATTEMPTS", 0)
	if err != nil {
		return appConfig{}, err
	}
	if maxDelay < initialDelay {
		return appConfig{}, fmt.Errorf("CDC_RETRY_MAX_DELAY deve ser maior ou igual a CDC_RETRY_INITIAL_DELAY")
	}
	pauseBeforeAck, err := boolFromEnv("CDC_TEST_PAUSE_BEFORE_ACK", false)
	if err != nil {
		return appConfig{}, err
	}
	pauseAfterSinkCommit, err := boolFromEnv("CDC_TEST_PAUSE_AFTER_SINK_COMMIT", false)
	if err != nil {
		return appConfig{}, err
	}
	enableHealth, err := boolFromEnv("CDC_HEALTH_ENABLED", true)
	if err != nil {
		return appConfig{}, err
	}
	publicationAutoConfigure, err := boolFromEnv("CDC_PUBLICATION_AUTOCONFIGURE", false)
	if err != nil {
		return appConfig{}, err
	}
	includedTables, err := parseTableList(envOrDefault("CDC_TABLE_INCLUDE", defaultTableInclude))
	if err != nil {
		return appConfig{}, err
	}

	sourceConfig, err := sourcePGConfig()
	if err != nil {
		return appConfig{}, err
	}

	sinkName := envOrDefault("CDC_SINK", defaultSink)
	var destConfig *pgx.ConnConfig
	if sinkName == "postgres" {
		destConfig, err = destinationPGConfig()
		if err != nil {
			return appConfig{}, err
		}
	}

	cfg := appConfig{
		sourceID:                 envOrDefault("CDC_SOURCE_ID", defaultSourceID),
		slotName:                 envOrDefault("CDC_SLOT", defaultSlotName),
		publication:              envOrDefault("CDC_PUBLICATION", defaultPublication),
		outputFileName:           envOrDefault("CDC_OUTPUT_FILE", defaultOutputFileName),
		statusInterval:           statusInterval,
		sourceConfig:             sourceConfig,
		destConfig:               destConfig,
		sinkName:                 sinkName,
		postgresApplyMode:        envOrDefault("CDC_POSTGRES_APPLY_MODE", defaultPostgresApplyMode),
		includedTables:           includedTables,
		publicationAutoConfigure: publicationAutoConfigure,
		retry: retryConfig{
			initialDelay: initialDelay,
			maxDelay:     maxDelay,
			maxAttempts:  maxAttempts,
		},
		healthAddr:     envOrDefault("CDC_HEALTH_ADDR", defaultHealthAddr),
		enableHealth:   enableHealth,
		pauseBeforeAck: pauseBeforeAck || pauseAfterSinkCommit,
		pauseMatch:     os.Getenv("CDC_TEST_PAUSE_MATCH"),
	}
	return cfg, validateConfig(cfg)
}

func validateConfig(cfg appConfig) error {
	if cfg.sourceID == "" {
		return fmt.Errorf("CDC_SOURCE_ID is required")
	}
	if cfg.slotName == "" {
		return fmt.Errorf("CDC_SLOT is required")
	}
	if cfg.publication == "" {
		return fmt.Errorf("CDC_PUBLICATION is required")
	}
	if cfg.sourceConfig.Host == "" {
		return fmt.Errorf("CDC_SOURCE_PGHOST is required")
	}
	if cfg.sourceConfig.Port == 0 {
		return fmt.Errorf("CDC_SOURCE_PGPORT is required")
	}
	if cfg.sourceConfig.Database == "" {
		return fmt.Errorf("CDC_SOURCE_PGDATABASE is required")
	}
	if cfg.sourceConfig.User == "" {
		return fmt.Errorf("CDC_SOURCE_PGUSER is required")
	}
	if cfg.sinkName != "file" && cfg.sinkName != "postgres" {
		return fmt.Errorf("CDC_SINK deve ser file ou postgres: %q", cfg.sinkName)
	}
	if cfg.sinkName == "file" && cfg.outputFileName == "" {
		return fmt.Errorf("CDC_OUTPUT_FILE is required")
	}
	if cfg.sinkName == "postgres" {
		if cfg.destConfig == nil {
			return fmt.Errorf("CDC_DEST_PGDATABASE is required")
		}
		if cfg.destConfig.Host == "" {
			return fmt.Errorf("CDC_DEST_PGHOST is required")
		}
		if cfg.destConfig.Port == 0 {
			return fmt.Errorf("CDC_DEST_PGPORT is required")
		}
		if cfg.destConfig.Database == "" {
			return fmt.Errorf("CDC_DEST_PGDATABASE is required")
		}
		if cfg.destConfig.User == "" {
			return fmt.Errorf("CDC_DEST_PGUSER is required")
		}
		if cfg.postgresApplyMode != "explicit" && cfg.postgresApplyMode != "generic" {
			return fmt.Errorf("CDC_POSTGRES_APPLY_MODE deve ser explicit ou generic: %q", cfg.postgresApplyMode)
		}
		if cfg.postgresApplyMode == "generic" && len(cfg.includedTables) == 0 {
			return fmt.Errorf("CDC_TABLE_INCLUDE e obrigatorio no modo generic")
		}
	}
	if cfg.publicationAutoConfigure && len(cfg.includedTables) == 0 {
		return fmt.Errorf("CDC_TABLE_INCLUDE e obrigatorio com CDC_PUBLICATION_AUTOCONFIGURE=true")
	}
	return nil
}

func sourcePGConfig() (*pgx.ConnConfig, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%s dbname=%s user=%s password=%s sslmode=%s",
		envOrDefault("CDC_SOURCE_PGHOST", "localhost"),
		envOrDefault("CDC_SOURCE_PGPORT", "5432"),
		os.Getenv("CDC_SOURCE_PGDATABASE"),
		os.Getenv("CDC_SOURCE_PGUSER"),
		os.Getenv("CDC_SOURCE_PGPASSWORD"),
		envOrDefault("CDC_SOURCE_PGSSLMODE", "disable"),
	)
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("configuracao da conexao source: %w", err)
	}
	return config, nil
}

func destinationPGConfig() (*pgx.ConnConfig, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%s dbname=%s user=%s password=%s sslmode=%s",
		envOrDefault("CDC_DEST_PGHOST", "localhost"),
		envOrDefault("CDC_DEST_PGPORT", "5432"),
		os.Getenv("CDC_DEST_PGDATABASE"),
		os.Getenv("CDC_DEST_PGUSER"),
		os.Getenv("CDC_DEST_PGPASSWORD"),
		envOrDefault("CDC_DEST_PGSSLMODE", "disable"),
	)
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("configuracao da conexao destination: %w", err)
	}
	return config, nil
}

func intFromEnv(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s deve ser um inteiro maior ou igual a zero: %q", name, value)
	}
	return parsed, nil
}
