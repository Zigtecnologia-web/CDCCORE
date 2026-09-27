SELECT 'CREATE DATABASE cdc_demo'
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = 'cdc_demo'
)\gexec

\connect cdc_demo

CREATE TABLE IF NOT EXISTS clientes (
    id BIGSERIAL PRIMARY KEY,
    nome TEXT NOT NULL,
    email TEXT
);

CREATE TABLE IF NOT EXISTS enderecos (
    id BIGSERIAL PRIMARY KEY,
    cliente_id BIGINT NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
    logradouro TEXT NOT NULL,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cidade TEXT NOT NULL,
    estado CHAR(2) NOT NULL,
    cep TEXT
);

CREATE INDEX IF NOT EXISTS enderecos_cliente_id_idx
ON enderecos (cliente_id);

ALTER TABLE clientes REPLICA IDENTITY FULL;
ALTER TABLE enderecos REPLICA IDENTITY FULL;

CREATE PUBLICATION cdc_publication
FOR TABLE public.clientes, public.enderecos;
