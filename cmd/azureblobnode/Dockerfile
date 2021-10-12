FROM golang:alpine AS builder
ENV GOBIN $GOPATH/bin
RUN apk update
RUN apk add git
RUN apk --no-cache add ca-certificates

# Set necessary environmet variables needed for our image
ENV GO111MODULE=auto \
    #CGO_ENABLED=1 \
    GOOS=linux \
    GOARCH=amd64

# Move to working directory /build
WORKDIR /build

# Copy and download dependency using go mod
COPY go.mod .
COPY go.sum .
RUN go mod download

# Copy the code into the container
COPY ./node.go ./node.go
COPY ./keyvault.go ./keyvault.go
COPY ./core/node ./core/node
COPY ./core/datasetmanager ./core/datasetmanager
COPY ./core/comm ./core/comm
COPY ./core/message ./core/message
COPY ./core/netutil ./core/netutil
COPY ./core/util ./core/util
COPY ./core/policyobserver ./core/policyobserver
COPY ./protobuf ./protobuf
COPY ./core/setsync ./core/setsync
COPY ./cmd/azureblobnode/main.go ./cmd/azureblobnode/main.go

# Run test
#RUN go test ./...

# Build the application
#RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .
RUN go build -o node ./cmd/azureblobnode/main.go

# Move to /dist directory as the place for resulting binary folder
WORKDIR /dist

# Copy binary from build to main folder
RUN cp /build/node .

############################
# STEP 2 build a small image
############################
FROM alpine
COPY --from=builder /dist/node /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the configuration files
COPY ./cmd/azureblobnode/config/ifrit_config.yaml /var/tmp/ifrit_config.yaml
COPY ./cmd/azureblobnode/config/lohpi_config.dev.yaml ./config/lohpi_config.dev.yaml
COPY ./cmd/azureblobnode/config/lohpi_config.yaml ./config/lohpi_config.yaml
COPY ./cmd/azureblobnode/crypto/production/lohpi/owncert.pem crypto/production/lohpi/owncert.pem
COPY ./cmd/azureblobnode/crypto/production/lohpi/cacert.pem crypto/production/lohpi/cacert.pem
COPY ./cmd/azureblobnode/crypto/production/lohpi/key.pem crypto/production/lohpi/key.pem
COPY ./cmd/azureblobnode/crypto/production/ifrit/*.pem crypto/production/ifrit/

# Command to run the executable
ENTRYPOINT ["/node", "-c", "config/lohpi_config.yaml"]

EXPOSE 8000 9000 5000