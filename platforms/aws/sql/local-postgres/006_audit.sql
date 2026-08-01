CREATE TABLE IF NOT EXISTS audit.warehouse_load_events (
  "EventID" text PRIMARY KEY,
  "LoadAttemptID" text NOT NULL,
  "BatchID" text NOT NULL,
  "CurationAttemptID" text,
  "EventTimestamp" timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
  "EventType" text NOT NULL CHECK ("EventType" IN ('STARTED', 'COMPLETED', 'NO_OP', 'CONFLICT', 'FAILED')),
  "PublicationFingerprint" text CHECK ("PublicationFingerprint" IS NULL OR "PublicationFingerprint" ~ '^[0-9a-f]{64}$'),
  "MarkerSHA256" text CHECK ("MarkerSHA256" IS NULL OR "MarkerSHA256" ~ '^[0-9a-f]{64}$'),
  "ContractVersion" integer,
  "PipelineVersion" text,
  "LoaderDetails" jsonb NOT NULL,
  "Details" jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS audit.completed_publications (
  "BatchID" text PRIMARY KEY,
  "PublicationFingerprint" text NOT NULL UNIQUE CHECK ("PublicationFingerprint" ~ '^[0-9a-f]{64}$'),
  "ContractVersion" integer NOT NULL,
  "PipelineVersion" text NOT NULL,
  "MarkerSHA256" text NOT NULL CHECK ("MarkerSHA256" ~ '^[0-9a-f]{64}$'),
  "DatasetEvidence" jsonb NOT NULL,
  "CompletedEventID" text NOT NULL UNIQUE REFERENCES audit.warehouse_load_events ("EventID"),
  "CompletedAt" timestamp with time zone NOT NULL
);

CREATE TABLE IF NOT EXISTS audit.current_snapshot (
  "Singleton" boolean PRIMARY KEY DEFAULT true CHECK ("Singleton"),
  "BatchID" text NOT NULL REFERENCES audit.completed_publications ("BatchID"),
  "PublicationFingerprint" text NOT NULL,
  "PublishedAt" timestamp with time zone NOT NULL
);

CREATE OR REPLACE FUNCTION audit.reject_immutable_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION '% is immutable', TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
END;
$$;

DROP TRIGGER IF EXISTS warehouse_load_events_immutable ON audit.warehouse_load_events;
CREATE TRIGGER warehouse_load_events_immutable
BEFORE UPDATE OR DELETE ON audit.warehouse_load_events
FOR EACH ROW EXECUTE FUNCTION audit.reject_immutable_mutation();

DROP TRIGGER IF EXISTS completed_publications_immutable ON audit.completed_publications;
CREATE TRIGGER completed_publications_immutable
BEFORE UPDATE OR DELETE ON audit.completed_publications
FOR EACH ROW EXECUTE FUNCTION audit.reject_immutable_mutation();

CREATE INDEX IF NOT EXISTS ix_warehouse_load_events_batch ON audit.warehouse_load_events ("BatchID");
CREATE INDEX IF NOT EXISTS ix_warehouse_load_events_attempt ON audit.warehouse_load_events ("LoadAttemptID");
CREATE INDEX IF NOT EXISTS ix_warehouse_load_events_type ON audit.warehouse_load_events ("EventType");
CREATE INDEX IF NOT EXISTS ix_warehouse_load_events_fingerprint ON audit.warehouse_load_events ("PublicationFingerprint");

SELECT format('GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA staging TO %I', :'loader_role') \gexec
SELECT format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA warehouse TO %I', :'loader_role') \gexec
SELECT format('GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA analytics TO %I', :'loader_role') \gexec
SELECT format('GRANT SELECT, INSERT ON audit.warehouse_load_events, audit.completed_publications TO %I', :'loader_role') \gexec
SELECT format('GRANT SELECT, INSERT, UPDATE ON audit.current_snapshot TO %I', :'loader_role') \gexec
SELECT format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA warehouse TO %I', :'loader_role') \gexec
