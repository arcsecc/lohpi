FROM golang:alpine AS builder
ENV GOBIN $GOPATH/bin
RUN apk update
RUN apk add git
RUN apk --no-cache add ca-certificates

# Set necessary environmet variables needed for our image
ENV GO111MODULE=auto \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Move to working directory /build
WORKDIR /build

# Copy and download dependency using go mod
COPY go.mod .
COPY go.sum .
RUN go mod download

# Local dependencies
COPY ./policy.go ./policy.go
COPY ./core/datasetmanager ./core/datasetmanager
COPY ./core/membershipmanager ./core/membershipmanager
COPY ./core/message ./core/message
COPY ./core/netutil ./core/netutil
COPY ./core/comm ./core/comm
COPY ./core/setsync ./core/setsync
COPY ./core/policyobserver ./core/policyobserver
COPY ./core/policystore ./core/policystore
COPY ./protobuf ./protobuf
COPY ./core/util ./core/util
COPY ./keyvault.go ./keyvault.go
COPY ./cmd/policy_store/launcher.go ./cmd/policy_store/launcher.go
COPY ./cmd/policy_store/config/ifrit_config.yaml ./cmd/policy_store/config/ifrit_config.yaml
COPY ./cmd/policy_store/config/lohpi_config.eval.yaml ./cmd/policy_store/config/lohpi_config.eval.yaml


# Run tests
#RUN go test ./...

# Build the application
#RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .
RUN go build -o policy_store ./cmd/policy_store/launcher.go

# Move to /dist directory as the place for resulting binary folder
WORKDIR /dist

# Copy binary from build to main folder
RUN cp /build/policy_store .

############################
# STEP 2 build a small image
############################
FROM scratch
COPY --from=builder /dist/policy_store /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY ./cmd/policy_store/config/ifrit_config.yaml /var/tmp/ifrit_config.yaml
COPY ./cmd/policy_store/config/lohpi_config.eval.yaml ./config/lohpi_config.eval.yaml

# Command to run the executable
ENTRYPOINT ["/policy_store", "-c", "config/lohpi_config.eval.yaml", "-new", "true"]

EXPOSE 8083 8084 5000 8000