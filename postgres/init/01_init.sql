-- Create Debezium user with replication privileges
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
    CREATE ROLE debezium WITH LOGIN PASSWORD 'debezium' REPLICATION;
  END IF;
END$$;

-- Basic sample table
CREATE TABLE IF NOT EXISTS public.orders (
  order_id      SERIAL PRIMARY KEY,
  user_id       INT NOT NULL,
  amount_eur    NUMERIC(10,2) NOT NULL,
  status        TEXT NOT NULL,
  created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Give Debezium enough privileges for this demo
GRANT CONNECT ON DATABASE app TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
