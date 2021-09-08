CREATE SCHEMA IF NOT EXISTS node1_schema;
CREATE TABLE IF NOT EXISTS node1_schema.node1_policy_table (
	"id" SERIAL PRIMARY KEY,
	"dataset_id" VARCHAR NOT NULL UNIQUE,
	"allow_multiple_checkout" BOOLEAN NOT NULL,
	"policy_content" BOOLEAN NOT NULL,
	"policy_version" INT NOT NULL,
	"date_created" VARCHAR NOT NULL,
	"date_applied" VARCHAR NOT NULL
);

create table IF NOT EXISTS node1_schema.node1_dataset_checkout_table (
 	"id" SERIAL PRIMARY KEY,
	"dataset_id" VARCHAR NOT NULL REFERENCES node1_schema.node1_policy_table(dataset_id),
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
CREATE TABLE IF NOT EXISTS node1_schema.node1_gossip_observation_table (
	"id" SERIAL PRIMARY KEY,
	"sequence_number" int NOT NULL,
	"date_received" VARCHAR NOT NULL,
	"policy_store_id" VARCHAR NOT NULL,
	"date_sent" VARCHAR NOT NULL
);

-- T2 table
CREATE TABLE IF NOT EXISTS node1_schema.node1_applied_policy_table (
	"id" SERIAL PRIMARY KEY,
 	"dataset_id" VARCHAR NOT NULL REFERENCES node1_schema.node1_policy_table(dataset_id),
 	"policy_version" INT NOT NULL,
 	"date_applied" VARCHAR, -- local time of when a policy was applied at the node
 	"date_issued" VARCHAR -- time from the PS as indicated in the gossip message, can be referenced from T1
);

-- Node #2
CREATE SCHEMA IF NOT EXISTS node2_schema;
CREATE TABLE IF NOT EXISTS node2_schema.node2_policy_table (
	"id" SERIAL PRIMARY KEY,
	"dataset_id" VARCHAR NOT NULL UNIQUE,
	"allow_multiple_checkout" BOOLEAN NOT NULL,
	"policy_content" BOOLEAN NOT NULL,
	"policy_version" INT NOT NULL,
	"date_created" VARCHAR NOT NULL,
	"date_applied" VARCHAR NOT NULL
);

create table IF NOT EXISTS node2_schema.node2_dataset_checkout_table (
 	"id" SERIAL PRIMARY KEY,
	"dataset_id" VARCHAR NOT NULL REFERENCES node2_schema.node2_policy_table(dataset_id),
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
CREATE TABLE IF NOT EXISTS node2_schema.node2_gossip_observation_table (
	"id" SERIAL PRIMARY KEY,
	"sequence_number" int NOT NULL,
	"date_received" VARCHAR NOT NULL,
	"policy_store_id" VARCHAR NOT NULL,
	"date_sent" VARCHAR NOT NULL
);

-- T2 table
CREATE TABLE IF NOT EXISTS node2_schema.node2_applied_policy_table (
	"id" SERIAL PRIMARY KEY,
 	"dataset_id" VARCHAR NOT NULL REFERENCES node2_schema.node2_policy_table(dataset_id),
 	"policy_version" INT NOT NULL,
 	"date_applied" VARCHAR, -- local time of when a policy was applied at the node
 	"date_issued" VARCHAR -- time from the PS as indicated in the gossip message, can be referenced from T1
);

-- Node #3
CREATE SCHEMA IF NOT EXISTS node3_schema;
CREATE TABLE IF NOT EXISTS node3_schema.node3_policy_table (
	"id" SERIAL PRIMARY KEY,
	"dataset_id" VARCHAR NOT NULL UNIQUE,
	"allow_multiple_checkout" BOOLEAN NOT NULL,
	"policy_content" BOOLEAN NOT NULL,
	"policy_version" INT NOT NULL,
	"date_created" VARCHAR NOT NULL,
	"date_applied" VARCHAR NOT NULL
);

create table IF NOT EXISTS node3_schema.node3_dataset_checkout_table (
 	"id" SERIAL PRIMARY KEY,
	"dataset_id" VARCHAR NOT NULL REFERENCES node3_schema.node3_policy_table(dataset_id),
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
CREATE TABLE IF NOT EXISTS node3_schema.node3_gossip_observation_table (
	"id" SERIAL PRIMARY KEY,
	"sequence_number" int NOT NULL,
	"date_received" VARCHAR NOT NULL,
	"policy_store_id" VARCHAR NOT NULL,
	"date_sent" VARCHAR NOT NULL
);

-- T2 table
CREATE TABLE IF NOT EXISTS node3_schema.node3_applied_policy_table (
	"id" SERIAL PRIMARY KEY,
 	"dataset_id" VARCHAR NOT NULL REFERENCES node3_schema.node3_policy_table(dataset_id),
 	"policy_version" INT NOT NULL,
 	"date_applied" VARCHAR, -- local time of when a policy was applied at the node
 	"date_issued" VARCHAR -- time from the PS as indicated in the gossip message, can be referenced from T1
);