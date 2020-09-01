FROM golang:alpine AS builder
RUN apk update
RUN apk add git

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

# Copy the code into the container
COPY ./cmd/mux/main.go .
COPY ./pkg/netutil/ ./pkg/netutil
COPY ./pkg/mux/ ./pkg/mux/
COPY ./pkg/node/ ./pkg/node/
COPY ./pkg/message/ ./pkg/message/
COPY ./pkg/cache/ ./pkg/cache/
COPY ./protobuf/ ./protobuf/
COPY ./pkg/comm/ ./pkg/comm/

# Run test
#RUN go test ./...

# Build the application
#RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .
RUN go build -o ca .

# Move to /dist directory as the place for resulting binary folder
WORKDIR /dist

# Copy binary from build to main folder
RUN cp /build/ca .

############################
# STEP 2 build a small image
############################
FROM scratch

COPY --from=builder /dist/ca /

# Copy the configuration files
COPY cmd/mux/config.yml .
COPY configs/ifrit_config.yaml /var/tmp/ifrit_config
COPY configs/lohpi_ca_config /var/tmp/lohpi_ca_config

# Command to run the executable
ENTRYPOINT ["/ca"]

EXPOSE 8300 8301


