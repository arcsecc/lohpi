#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'
    DO
    $do$
    BEGIN
       IF NOT EXISTS (
          SELECT FROM pg_catalog.pg_roles  -- SELECT list can be empty for this
          WHERE  rolname = 'lohpi_dev_user') THEN

          CREATE ROLE lohpi_dev_user  WITH ENCRYPTED PASSWORD 'password';
       END IF;
    END
    $do$;

    -- Create the database used for development
    SELECT 'CREATE DATABASE directory_server_db'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'directory_server_db')\gexec

    -- Set dummy password
    --ALTER USER lohpi_dev_user PASSWORD 'password!';

    -- Assign user to DATABASEs
    GRANT ALL PRIVILEGES ON DATABASE directory_server_db TO lohpi_dev_user;

    ALTER ROLE "lohpi_dev_user" WITH LOGIN;

    CREATE SCHEMA IF NOT EXISTS directory_server_schema;
    CREATE TABLE IF NOT EXISTS directory_server_schema.directory_server_storage_node_table  (
    	"id" SERIAL PRIMARY KEY,
    	"node_name" VARCHAR NOT NULL UNIQUE,
    	"ip_address" VARCHAR NOT NULL,
    	"public_id" BYTEA NOT NULL,
    	"https_address" VARCHAR NOT NULL,
    	"port" INT NOT NULL,
    	"boottime" VARCHAR NOT NULL
    );
    
    -- Lookup table
    CREATE TABLE IF NOT EXISTS directory_server_schema.directory_server_dataset_lookup_table (
    	"id" SERIAL PRIMARY KEY,
    	"dataset_id" VARCHAR NOT NULL UNIQUE,
    	"node_name"	VARCHAR NOT NULL REFERENCES directory_server_schema.directory_server_storage_node_table(node_name)
    );
    
    CREATE TABLE IF NOT EXISTS directory_server_schema.directory_server_dataset_checkout_table (
        "id" SERIAL PRIMARY KEY,
    	"dataset_id" VARCHAR NOT NULL,
    	"client_id" VARCHAR NOT NULL,	
    	"client_name" VARCHAR NOT NULL,
    	"dns_name" VARCHAR NOT NULL,
    	"mac_address" VARCHAR,
    	"ip_address" VARCHAR,
    	"email_address" VARCHAR NOT NULL,
    	"date_checkout" VARCHAR NOT NULL,
    	"policy_version" INT NOT NULL
    );
    
    -- T1 table
    CREATE TABLE IF NOT EXISTS directory_server_schema.directory_server_gossip_observation_table (
    	"id" SERIAL PRIMARY KEY,
    	"sequence_number" int NOT NULL,
    	"date_received" VARCHAR NOT NULL,
    	"policy_store_id" BYTEA NOT NULL,
    	"date_sent" VARCHAR NOT NULL
    );
EOSQL