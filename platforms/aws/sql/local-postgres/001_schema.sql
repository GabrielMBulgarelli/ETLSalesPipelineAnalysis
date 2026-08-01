SET timezone = 'UTC';

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS audit;

SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'loader_role') \gexec
SELECT format('GRANT USAGE ON SCHEMA staging, warehouse, analytics, audit TO %I', :'loader_role') \gexec
SELECT format('ALTER ROLE %I SET timezone = %L', :'loader_role', 'UTC') \gexec
