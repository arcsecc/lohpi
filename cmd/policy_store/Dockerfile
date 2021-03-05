FROM golang:alpine AS builder

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
COPY ./cmd/policy_store/launcher.go .
COPY ./core/netutil/ ./core/netutil
COPY ./core/policy/ ./core/policy/
COPY ./core/comm/ ./core/comm/
COPY ./core/cache/ ./core/cache/
COPY ./core/util/ ./core/util/
COPY ./core/message/ ./core/message/
COPY ./protobuf/ ./protobuf
COPY ./cauth/ ./cauth
#COPY ./cmd/policy_store/BaltimoreCyberTrustRoot.crt.pem .

# Run test
#RUN go test ./...

# Build the application
#RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .
RUN go build -o policy_store .

# Move to /dist directory as the place for resulting binary folder
WORKDIR /dist

# Copy binary from build to main folder
RUN cp /build/policy_store .

############################
# STEP 2 build a small image
############################
FROM scratch

ENV POLICY_STORE_DB_CONNECTION_STRING $POLICY_STORE_DB_CONNECTION_STRING
COPY --from=builder /dist/policy_store /

# Copy the configuration files
COPY configs/ifrit_config.yaml /var/tmp/ifrit_config
COPY cmd/policy_store/config.yml .
COPY ./cmd/policy_store/BaltimoreCyberTrustRoot.crt.pem .

# Command to run the executable
ENTRYPOINT ["/policy_store"]

EXPOSE 8300 8301


