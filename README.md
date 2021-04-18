# Lohpi

Lohpi is a distributed system for sharing and enforcing data policies. By bootstrapping storage nodes close to existing infrastructure, 
data access is enforced and tracked by employing Azure AD identities and PostgresQL. It uses the underlying [Ifrit](https://www.github.com/joonnna/ifrit) library for message passing and secure membership management.

Lohpi consists of multiple components, each of which is explained below. In addition, each component exposes and API that can be consumed. 

#### Directory server
The directory server is a network entity that serves client requests and forwards them to the correct storage node. The directory server keeps track of the datasets that are stored at the nodes and which datasets that have been checked out by clients.

#### Storage node
A storage node is a node that assigns policies with dataset identifiers. In addition, it enforces accesses and keeps track of the datasets being checked out by the clients. There can exist multiple nodes, each is assigned a data repository to safeguard data. 

#### Policy store
The policy store is a component that accepts policy changes through a web interface and disseminates the policies in batches using multicasting and gossiping.

#### CA
The certificate authority is an entity that distributes X.509 certificates upon requests from the storage nodes and the underlying Ifrit network. 

# Production environment
To deploy the system at the diggi-2 server, run ```docker-compose up```. This, however, whould not be necessary because the Github runner will run it whenever changes are pushed to the master branch. When a new deployment is done, run ```docker ps``` to check if any containers try to restart. If one or more tries to restart, something went wrong. 

# Local development
These are the steps needed to create a local development database on your own machine.
Please note that this does not apply running it on any diggi servers that already hosts Dataverse.
You need to connect to VPN if you are outside of the university domain. 

1. Start a PostgreSQL DBMS instance running in Docker
```$ docker run -d --name lohpi-postgres-dev -e POSTGRES_PASSWORD=password! -v ${HOME}/postgres-data/:/var/lib/postgresql/data -p 5432:5432 postgres```

2. Run docker ps to see that it runs. Note the container ID to tear it down later using ```docker stop <id>```.

3. Copy dev-initialize.sql into the Docker image:
```$ docker cp dev-initalize.sql lohpi-postgres-dev:/dev-initialize.sql```

4. Enter the shell:
```$ docker exec -it lohpi-postgres-dev bash```

5. Setup the development database:
```$ psql -h localhost -U postgres -f dev-initialize.sql```

6. When you need to login to the database using psql, run 
```$ psql -h localhost -U lohpi_dev_user -d dataset_policy_db```. Now you can operate directly on the DBMS using the psql shell, if needed. 

7. Tear down the database by running ```docker stop <id>``` as explained above. 

#### Docker-compose 
You can deploy the system locally on your system using ```docker-compose -f docker-compose.dev.yml build``` and ```docker-compose -f docker-compose.dev.yml up```. This will build the CA, directory server, policy store and one storage node. If you want muliple nodes, you can build additional images by running ```docker build -f cmd/dataverse/node/Dockerfile -t some_tag``` and then ```docker run --network=host -t some_tag```. Remember to supply a unique value to the ```-name``` flag in the node executable.

#### Go executables
You can compile the programs separately by running ```export LOHPI_ENV=development``` and then running ```go build``` in the ```cmd/dataverse``` subdirectories. 

## Lohpi roadmap
To be completed
