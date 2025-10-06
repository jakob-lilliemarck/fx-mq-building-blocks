-- Add down migration script here

-- Drop tables in reverse order of creation, respecting foreign key dependencies
-- First drop tables that reference others
DROP TABLE IF EXISTS errors;
DROP TABLE IF EXISTS attempts_dead;
DROP TABLE IF EXISTS attempts_succeeded;
DROP TABLE IF EXISTS attempts_failed;

-- Then drop tables without dependencies
DROP TABLE IF EXISTS leases;

-- Finally drop the referenced table
DROP TABLE IF EXISTS messages_attempted;
DROP TABLE IF EXISTS messages_unattempted;
