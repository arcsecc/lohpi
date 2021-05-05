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
COPY ./directoryserver.go ./directoryserver.go
COPY ./core/directoryserver ./core/directoryserver
COPY ./core/cache ./core/cache
COPY ./core/message ./core/message
COPY ./core/netutil ./core/netutil
COPY ./core/comm ./core/comm
COPY ./protobuf ./protobuf
COPY ./cmd/dataverse/directoryserver/main.go ./cmd/dataverse/directoryserver/main.go
COPY ./cmd/dataverse/directoryserver/lohpi_config.yaml ./cmd/dataverse/directoryserver/lohpi_config.yaml
COPY ./cmd/dataverse/directoryserver/ifrit_config.yaml ./cmd/dataverse/directoryserver/ifrit_config.yaml

# Run test
#RUN go test ./...

# Build the application
#RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .
RUN go build -o directoryserver ./cmd/dataverse/directoryserver/main.go 

# Move to /dist directory as the place for resulting binary folder
WORKDIR /dist

# Copy binary from build to main folder
RUN cp /build/directoryserver .

############################
# STEP 2 build a small image
############################
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /dist/directoryserver /
COPY ./cmd/dataverse/directoryserver/ifrit_config.yaml /var/tmp/ifrit_config.yaml
COPY ./cmd/dataverse/directoryserver/lohpi_config.yaml .

# Command to run the executable
ENTRYPOINT ["/directoryserver"]
