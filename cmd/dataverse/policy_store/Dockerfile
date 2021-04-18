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

# Run test
#RUN go test ./...

# Build the application
#RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .
COPY ./policy.go ./policy.go
COPY ./core ./core
COPY ./protobuf ./protobuf
COPY ./cmd/dataverse/policy_store/launcher.go ./cmd/dataverse/policy_store/launcher.go
COPY ./cmd/dataverse/policy_store/lohpi_config.yaml ./cmd/dataverse/policy_store/lohpi_config.yaml
RUN go build -o policy_store ./cmd/dataverse/policy_store/launcher.go

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

# Copy the configuration files
COPY ./cmd/dataverse/policy_store/ifrit_config.yaml /var/tmp/ifrit_config.yaml
COPY ./cmd/dataverse/policy_store/lohpi_config.yaml .

# Command to run the executable
ENTRYPOINT ["/policy_store"]


