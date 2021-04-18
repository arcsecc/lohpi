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
SELECT 'CREATE DATABASE dataset_policy_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dataset_policy_db')\gexec

-- Set dummy password
--ALTER USER lohpi_dev_user PASSWORD 'password!';

-- Assign user to DATABASE
GRANT ALL PRIVILEGES ON DATABASE dataset_policy_db TO lohpi_dev_user;

ALTER ROLE "lohpi_dev_user" WITH LOGIN;