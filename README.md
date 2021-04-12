These are the steps needed to create a local development database on your own machine.
Please note that this does not apply running it on any diggi servers that already hosts Dataverse.
You need to connect to VPN if you are outside of the university domain. 

1. Start a PostgreSQL DBMS instance running in Docker
$ docker run -d --name lohpi-postgres-dev -e POSTGRES_PASSWORD=password! -v ${HOME}/postgres-data/:/var/lib/postgresql/data -p 5432:5432 postgres

2. Run docker ps to see that it runs. Note the container ID to tear it down later using "docker stop <id>".

3. Copy dev-initialize.sql into the Docker image:
$ docker cp dev-initalize.sql lohpi-postgres-dev:/dev-initialize.sql

4. Enter the shell:
$ docker exec -it lohpi-postgres-dev bash

5. Setup the development database:
$ psql -h localhost -U postgres -f dev-initialize.sql

6. When you need to login to the database using psql, run 
$ psql -h localhost -U lohpi_dev -d dataset_policy_db
Now you can operate directly on the DBMS using the psql shell, if needed. 

7. Tear down the database by running ```docker stop <id>``` as explained above. 