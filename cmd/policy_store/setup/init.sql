CREATE SCHEMA IF NOT EXISTS policy_store_schema;
CREATE TABLE IF NOT EXISTS policy_store_schema.policy_store_storage_node_table (
	"id" SERIAL PRIMARY KEY,
	"node_name" VARCHAR NOT NULL UNIQUE,
	"ip_address" VARCHAR NOT NULL UNIQUE,
	"public_id" BYTEA NOT NULL,
	"https_address" VARCHAR NOT NULL,
	"port" INT NOT NULL,
	"boottime" VARCHAR NOT NULL
	--"timezone" VARCHAR NOT NULL
);

-- Directory server setup for dataset checkout
CREATE TABLE IF NOT EXISTS policy_store_schema.policy_store_policy_table (
	"id" SERIAL PRIMARY KEY,
	"dataset_id" VARCHAR NOT NULL UNIQUE,
	"allow_multiple_checkout" BOOLEAN NOT NULL,
	"policy_content" BOOLEAN NOT NULL,
	"policy_version" INT NOT NULL,
	"date_created" VARCHAR NOT NULL, -- Arrived at PS. change this at the node
	"date_updated" VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_store_schema.policy_store_dataset_lookup_table (
	"id" SERIAL PRIMARY KEY,
	"dataset_id" VARCHAR NOT NULL UNIQUE REFERENCES policy_store_schema.policy_store_policy_table(dataset_id),
	"node_name" VARCHAR NOT NULL REFERENCES policy_store_schema.policy_store_storage_node_table(node_name)
);

CREATE TABLE IF NOT EXISTS policy_store_schema.policy_store_policy_dissemination_table ( -- inserted when PS sends a direct message to a recipient
	"id" SERIAL PRIMARY KEY,
	"recipient" VARCHAR NOT NULL REFERENCES policy_store_schema.policy_store_storage_node_table(ip_address),
	"date_sent" VARCHAR NOT NULL,
	"policy_store_id"  BYTEA NOT NULL,
	"sequence_number" INT NOT NULL,
	"local_timezone" VARCHAR NOT NULL,
	"time_offset" INT NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_store_schema.policy_store_direct_message_lookup_table ( -- inserted when PS creates/composes a message to be gossiped/sent
	"id" SERIAL PRIMARY KEY,
	"policy_store_id"  BYTEA NOT NULL,
	"sequence_number" INT NOT NULL,
	"dataset_id" VARCHAR NOT NULL REFERENCES policy_store_schema.policy_store_dataset_lookup_table(dataset_id),
	"policy_version" INT NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_store_schema.policy_store_gossip_observation_table (
	"id" SERIAL PRIMARY KEY,
	"sequence_number" int NOT NULL,
	"date_received" VARCHAR NOT NULL,
	"policy_store_id" BYTEA NOT NULL,
	"date_sent" VARCHAR NOT NULL -- set by PS
);

create table policy_store_schema.policy_store_policy_reconciliation_table (
    "id" serial primary key,
	"node_name" VARCHAR NOT NULL REFERENCES policy_store_schema.policy_store_storage_node_table(node_name),
    "timestamp" VARCHAR NOT NULL,
	"num_update" INT NOT NULL
);
