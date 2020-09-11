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
COPY ./pkg/netutil/ ./pkg/netutil
COPY ./pkg/policy/ ./pkg/policy/
COPY ./pkg/comm/ ./pkg/comm/
COPY ./pkg/cache/ ./pkg/cache/
COPY ./pkg/message/ ./pkg/message/
COPY ./protobuf/ ./protobuf
COPY ./cauth/ ./cauth

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

COPY --from=builder /dist/policy_store /

# Copy the configuration files
COPY configs/ifrit_config.yaml /var/tmp/ifrit_config
COPY cmd/policy_store/config.yml .

# Command to run the executable
ENTRYPOINT ["/policy_store"]

EXPOSE 8300 8301


