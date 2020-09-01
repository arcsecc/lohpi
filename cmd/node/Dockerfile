FROM golang:alpine AS builder
ENV GOBIN $GOPATH/bin
RUN apk update
RUN apk add git curl fuse-dev gcc libc-dev

# Set necessary environmet variables needed for our image
ENV GO111MODULE=auto \
    CGO_ENABLED=1 \
    GOOS=linux \
    GOARCH=amd64

# Move to working directory /build
WORKDIR /build

# Copy and download dependency using go mod
COPY go.mod .
COPY go.sum .
RUN go mod download

# Copy the code into the container
COPY ./cmd/node/launcher.go ./cmd/node/launcher.go
COPY ./pkg/netutil/ ./pkg/netutil
COPY ./pkg/message ./pkg/message
COPY ./pkg/mux ./pkg/mux
COPY ./pkg/node ./pkg/node
COPY ./pkg/policy ./pkg(policy
COPY ./pkg/rec ./pkg/rec
COPY ./protobuf/ ./protobuf
COPY ./pkg/comm ./pkg/comm

# Run test
#RUN go test ./...

# Build the application
#RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .
RUN go build -a -o main ./cmd/node/launcher.go

# Move to /dist directory as the place for resulting binary folder
WORKDIR /dist

# Copy binary from build to main folder
RUN cp /build/main .

############################
# STEP 2 build a small image
############################
FROM alpine
RUN apk update
RUN apk add fuse-dev 
COPY --from=builder /dist/main /

# Copy the configuration files
COPY configs/ifrit_config.yaml /var/tmp/ifrit_config
COPY cmd/node/config.yml .

# Command to run the executable
ENTRYPOINT ["/main"]