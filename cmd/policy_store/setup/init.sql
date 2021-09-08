CREATE SCHEMA IF NOT EXISTS policy_store_schema;
CREATE TABLE IF NOT EXISTS policy_store_schema.policy_store_storage_node_table (
	"id" SERIAL PRIMARY KEY,
	"node_name" VARCHAR NOT NULL UNIQUE,
	"ip_address" VARCHAR NOT NULL,
	"public_id" BYTEA NOT NULL,
	"https_address" VARCHAR NOT NULL,
	"port" INT NOT NULL,
	"boottime" VARCHAR NOT NULL
);

-- Directory server setup for dataset checkout
CREATE TABLE IF NOT EXISTS policy_store_schema.policy_store_policy_table (
	"id" SERIAL PRIMARY KEY,
	"dataset_id" VARCHAR NOT NULL UNIQUE,
	"allow_multiple_checkout" BOOLEAN NOT NULL,
	"policy_content" BOOLEAN NOT NULL,
	"policy_version" INT NOT NULL,
	"date_created" VARCHAR NOT NULL,
	"date_applied" VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_store_schema.policy_store_dataset_lookup_table (
	"id" SERIAL PRIMARY KEY,
	"dataset_id" VARCHAR NOT NULL UNIQUE REFERENCES policy_store_schema.policy_store_policy_table(dataset_id),
	"node_name" VARCHAR NOT NULL REFERENCES policy_store_schema.policy_store_storage_node_table(node_name)
);