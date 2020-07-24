# Lohpi

Lohpi is a distributed storage system enforcing file access policies. It uses network gossiping provided by [Ifrit](https://github.com/joonnna/ifrit). 

## API
All APIs are included in the `internal` directory. As of now, they are not `go get`-able, but they will be refactored into a public 
library in the future.

## Bootstrapping
In `cmd/procs` there is a Proof-of-Work implementation of the Lohpi system. Each system components requires a configuration file at boot
time. For the working local deployment implementation, start the CA. Then, run `procs` with the following options: 
`./procs -clients <k> -config config.yml -exec ./../node/node -nodeconfig ./../node/config.yml -logfile logfile -nodes <n> -port 8080`

**NOTE**: Running the system components in Docker containers is work in progress. 

### CA
Lohpi requires a running Ifrit CA running. Please refer to the [Ifrit](https://github.com/joonnna/ifrit) page on how to do that. Similar to configuring the
Ifrit CA, create the file `/var/tmp/lohpi_ca_config` and insert the following lines:
<br/>
<br/>
`certificate_filepath: <certificate file path>`
<br/>
`key_filepath: <key file path>`
<br/>
`port: <Lohpi CA port number>`

### Mux 
The Mux (short for multiplexer) is the main point of entry to the system that interacts with data and metadata requests. The following HTTP endpoints are available:
- `/network`: Prints human-readable information about the network.
- `/node/load`: Loads a specific node with randomized data, metadata and assoicates subjects to a study. The Mux and policy store will be updated with 
in-memory summary data about the study, subjects and nodes.
  - Example: `curl -X POST -F "metadata=@metadata.json" -F subjects='s1' -F subjects='s2' -F study="my_study" -F node="node_0" 127.0.1.1:8080/node/load`

### Policy store
Policy store stores policies in an append-only fashion using [Go Git](https://github.com/go-git/go-git). Updates from REC are stored and multicasted to the 
network nodes. To optimize policy disseminations, the policy store probes the network to find the optimal multicast configurations. Policy store has the 
following HTTP end-points:
- `/probe`: Puts policy updates to a halt and probes the network to find optimal network parameters.

### Node
Each node runs a FUSE daemon that stores and enforces policies.

### REC
A network entity representing the Regional Ethics Committee ([REK](https://helseforskning.etikkom.no/)). It pushed study policies to the policy store.

### 
