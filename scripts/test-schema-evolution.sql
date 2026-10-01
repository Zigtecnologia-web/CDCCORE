\set ON_ERROR_STOP on

BEGIN;

DO $test$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'clientes'
          AND column_name = 'telefone'
    ) THEN
        RAISE EXCEPTION 'public.clientes.telefone ja existe no source; remova a coluna antes de executar este teste';
    END IF;
END
$test$;

ALTER TABLE public.clientes
ADD COLUMN telefone VARCHAR(30);

INSERT INTO public.clientes (nome, email, telefone)
VALUES (
    'Teste Schema Evolution',
    'schema-evolution-' || txid_current() || '@example.com',
    '71999990000'
)
RETURNING id, nome, email, telefone;

COMMIT;
