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
COPY ./cmd/ca/launcher.go .
COPY ./pkg/netutil/ ./pkg/netutil
COPY ./cauth/ ./cauth

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
COPY configs/ca_config /var/tmp/ca_config
COPY configs/lohpi_ca_config /var/tmp/lohpi_ca_config

# Command to run the executable
ENTRYPOINT ["/ca"]

EXPOSE 8300 8301


