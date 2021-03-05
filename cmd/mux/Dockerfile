FROM golang:alpine AS builder
ENV GOBIN $GOPATH/bin
RUN apk update
RUN apk --no-cache add ca-certificates

#RUN apk add git

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

# Golang files
COPY ./cmd/mux/main.go .
COPY ./core/mux/ ./core/mux/
COPY ./core/netutil/ ./core/netutil
COPY ./core/policy/ ./core/policy/
COPY ./core/comm/ ./core/comm/
COPY ./core/cache/ ./core/cache/
COPY ./core/util/ ./core/util/
COPY ./core/message/ ./core/message/
COPY ./protobuf/ ./protobuf
COPY ./cauth/ ./cauth
#COPY ./cmd/mux/config.yml cmd/mux/config.yml

# Run test
#RUN go test ./...

# Build the application
#RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .
#RUN CGO_ENABLED=0 go- install -ldflags '-extldflags "-static"'
#RUN CGO_ENABLED=0 GOOS=linux go build -ldflags '-extldflags "-static"' -o mux .
RUN go build -o mux .

# Move to /dist directory as the place for resulting binary folder
WORKDIR /dist

# Copy binary from build to main folder
RUN cp /build/mux .

############################
# STEP 2 build a small image
############################
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /dist/mux /
COPY ./configs/ifrit_config.yaml /var/tmp/ifrit_config
COPY ./cmd/mux/config.yml cmd/mux/config.yml

# Command to run the executable
ENTRYPOINT ["/mux"]

EXPOSE 8080