-- Create dev user
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles  -- SELECT list can be empty for this
      WHERE  rolname = 'lohpi_dev_user') THEN

      CREATE ROLE lohpi_dev_user  WITH ENCRYPTED PASSWORD 'password!';
   END IF;
END
$do$;

-- Create the database used for development
SELECT 'CREATE DATABASE node_db_test'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'node_db_test')\gexec

SELECT 'CREATE DATABASE directory_server_db_test'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'directory_server_db_test')\gexec

SELECT 'CREATE DATABASE policy_store_db_test'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'policy_store_db_test')\gexec

-- Set dummy password
--ALTER USER lohpi_dev_user PASSWORD 'password!';

-- Assign user to DATABASEs
GRANT ALL PRIVILEGES ON DATABASE node_db_test TO lohpi_dev_user;
GRANT ALL PRIVILEGES ON DATABASE directory_server_db_test TO lohpi_dev_user;
GRANT ALL PRIVILEGES ON DATABASE policy_store_db_test TO lohpi_dev_user;

-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA directory_server_schema TO lohpi_dev_user;
--GRANT ALL PRIVILEGES ON TABLE directory_server_schema.dataset_table TO lohpi_dev_user;

-- Not working
--GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO lohpi_dev_user;
--GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO lohpi_dev_user;
--GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO lohpi_dev_user;

ALTER ROLE "lohpi_dev_user" WITH LOGIN;