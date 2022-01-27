# Lohpi

Lohpi is a distributed system for sharing and enforcing data policies. By bootstrapping storage nodes close to existing infrastructure, data access is enforced and tracked by employing Azure AD identities and PostgresQL. It uses the underlying [Ifrit](https://www.github.com/joonnna/ifrit) library for message passing and secure membership management.

The Lohpi network consists of the directory server, policy store and a number of nodes. All components communicate using the Ifrit API. This document describes setting up a local development environment, running tests and deployments in Docker.

## Building the components

In development, build the components by running ```go build``` in the sub-directories of ```./cmd```. For building the components, run ```./scripts/build_images_local.sh``` or ```./scripts/build_native_local.sh```. The node implementation is only for demonstrative purposes. You need to consume the Lohpi node API for a customized implementation. 

## Deployments

Deploying the components requires you to supply configurations. The configurations are described below:

* Directory server

```yaml
# HTTP server port
httpport:

# gRPC server port
grpcport: 

# String identifier
name:

# Hostname used in FQDN. Use "127.0.1.1" if you want to run it locally.
hostname:

#  Lohpi CA address
lohpicaaddress:

# Azure key vault
azurekeyvaultname:
azurekeyvaultsecret:
azureclientsecret:
azuretenantid:
azureclientid:
azurekeyvaultbaseurl:

# Ifrit's TCP and UDP ports
ifrittcpport:
ifritudpport:

# Crypto unit directories
lohpicryptounitworkingdirectory:
ifritcryptounitworkingdirectory:

# Database connection pool size
dbconnpoolsize:
```

* Policy store

```yaml
# HTTP server port
httpport:

# gRPC server port
grpcport: 

# String identifier
name:

# Hostname used in FQDN
hostname:

#  Lohpi CA address
lohpicaaddress:

# Azure key vault
azurekeyvaultname:
azurekeyvaultsecret:
azureclientsecret:
azuretenantid:
azureclientid:
azurekeyvaultbaseurl:

# Ifrit's TCP and UDP ports
ifrittcpport:
ifritudpport:

# Crypto unit directories
lohpicryptounitworkingdirectory:
ifritcryptounitworkingdirectory:

# Gossip interval
gossipinterval:

# Database connection pool size
dbconnpoolsize:

# Number of direct recipients. Sigma.
numdirectrecipients:
```

* Storage node

```yaml
# Known addresses for joining the network
lohpicaaddress:
directoryserveraddress: 
policystoreaddress: 

# Hostname used in FQDN. Use "127.0.1.1" if you want to run it locally.
hostname: 
port: 

# Unique identifier.
name: 

# Azure key vault
azurekeyvaultname: 
azurekeyvaultsecret: 
azureclientsecret: 
azuretenantid: 
azureclientid: 
azurekeyvaultbaseurl: 

# Ifrit's TCP and UDP ports
ifrittcpport: 
ifritudpport: 

# Crypto unit directories
lohpicryptounitworkingdirectory: 
ifritcryptounitworkingdirectory: 

# Database connection pool size
dbconnpoolsize:

# Interval between each round of policy reconciliation, measured in seconds
setreconciliationinterval: 
```

To deploy multiple nodes, the ```name``` field has to be unique to the network.

## Local development

To create a local development environment, run ```setup_local_development.sh``` from the project root directory. This will create a PostgreSQL database along with the nescessary schemas and tables.

To run the components, supply a configuration file in the subdirectories of ```./cmd```. Use ```dev```,```eval``` or ```demo``` schemas.

## Azure Container Registery

The container registery being used is located at ```lohpi.azurecr.io```. All YML deployment schemas consumed by ```az container create``` use that location to deploy containers. Push images to that registry. The deployment schemas are located in ```deployment/schemas```.

## Azure Container Instance deployments

To deploy Azure Container instances, use ```az container create``` and supply a ```.yml``` file as argument to the ```--file``` option.

## Lohpi directory server endpoints

Returns a JSON object with available datasets.

* **URL**\
dataset/identifiers

* **Method:**\
  `GET`

* **Success responses:**\
  **Code:** 200\
  **Content:**
  
  ```{
      "Identifiers" : [
        "dataset1",
        "dataset2",
        ...
      ]
    }
  ```

* **Error responses:**\
  **Code:** 500 Internal Server Error\
  **Content:** Internal server error occured while processing the request.

* **Sample Call:**\
  ```curl 127.0.1.1:8080/dataset/identifiers```

---

Returns the metadata for the specified dataset.

* **URL**\
  dataset/metadata/:id

* **Method:**\
  `GET`

* **URL Parameters required:**\
   Dataset identifier `id=[string]`

* **Success responses:**\
  **Code:** 200 OK\
  **Content:**

  ```{
      metadata: {
        ...
    }
  ```

* **Error responses:**\
  **Code:** 404 Not Found\
  **Content:** Metadata with identifier ```id``` could not be found.

  **Code:** 500 Internal Server Error\
  **Content:** Internal server error occured while processing the request.

  **Code:** 504 Not Implemented\
  **Content**: Metadata download handler is not implemented at the node that indexes the dataset.

* **Sample call:**\
  ```curl 127.0.1.1:8080/dataset/metadata/foo_dataset```

---

Returns the dataset, given the identifier.

* **URL**\
dataset/data/:id

* **Method**
  `GET`

* **URL parameters required:**\
  Dataset identifier `id=[string]`

* **Success response:**\
  **Code:** 200 OK\
  **Content:** Compressed archive
  
* **Error responses:**\
  **Code:** 404 Not Found\
  **Content:** Dataset with identifier ```id``` could not be found.

  **Code:** 500 Internal Server Error\
  **Content:** Internal server error occured while processing the request.

  **Code:** 504 Not Implemented\
  **Content:** Dataset download handler is not implemented at the node that indexes the dataset.

* **Sample call:**\
  ```curl 127.0.1.1:8080/dataset/data/foo_dataset```

---

Verifies wether or not the client is compliant with the dataset's current policy

* **URL**\
  dataset/comply/:id

* **Method:**\
  `GET`

* **URL Parameters required:**\
   Dataset identifier `id=[string]`

* **Success responses:**\
  **Code:** 200 OK\
  **Content:**

  ```
  {
      complies: true/false
  }
  ```

* **Error responses:**\
  **Code:** 404 Not Found\
  **Content:** The dataset with identifier ```id``` could not be found.

  **Code:** 500 Internal Server Error\
  **Content:** Internal server error occured while processing the request.

* **Sample call:**\
  ```curl 127.0.1.1:8080/dataset/comply/foo_dataset```

---

Checks the dataset back into Lohpi.

* **URL**\
  dataset/check_in/:id

* **Method:**\
  `GET`

* **URL Parameters required:**\
   Dataset identifier `id=[string]`

* **Success responses:**\
  **Code:** 200 OK\
  **Content:** Succsessfully checked in dataset ```id```.

* **Error responses:**\
  **Code:** 404 Not Found\
  **Content:** The dataset with identifier ```id``` is not checked out by the client.

  **Code:** 500 Internal Server Error\
  **Content:** Internal server error occured while processing the request.

* **Sample call:**\
  ```curl 127.0.1.1:8080/dataset/comply/foo_dataset```

---

Sets a new project description for a dataset.

* **URL**\
  dataset/set_project_description/:id

* **Method** `POST`

* **URL Parameters required:**\
    Dataset identifier `id=[string]`

* **Success responses:**
  **Code:** 200 OK\
  **Content:** Succsessfully updated project description for dataset ```id```.
  
* **Error responses:**
  **Code:** 404 Not Found\
  **Content:** Dataset with identifier ```id``` could not be found.

  **Code:** 500 Internal Server Error\
  **Content:** Internal server error occured while processing the request.

* **Sample call:**\
  ```curl 127.0.1.1:8080/dataset/set_project_description/foo_dataset```

---

Gets the project description for a dataset.

* **URL**\
  dataset/get_project_description/:id

* **Method** `GET`

* **URL Parameters required:**\
   Dataset identifier `id=[string]`

* **Success Response:**
  * **Code:** 200 OK\
  * **Content:**

  ```{
      project_description: {
        ...
    }
  ```
  
* **Error Response:**
  **Code:** 404 Not Found\
  **Content:** Dataset with identifier ```id``` could not be found.

  **Code:** 500 Internal Server Error\
  **Content:** Internal server error occured while processing the request.

* **Sample Call:**\
  ```curl 127.0.1.1:8080/dataset/Get_project_description/foo_dataset```

## Lohpi policy store endpoints