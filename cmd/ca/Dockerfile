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
    #GOPATH=/go

# Move to working directory /build
WORKDIR /build

# Copy and download dependency using go mod
COPY go.mod .
COPY go.sum .
RUN go mod download

# Local dependencies
COPY ./cauth ./cauth
COPY ./cmd/ca/main.go ./cmd/ca/main.go

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ca ./cmd/ca/main.go
#RUN go build -o ca ./cmd/ca/main.go

# Move to /dist directory as the place for resulting binary folder
WORKDIR /dist

# Copy binary from build to main folder
RUN cp /build/ca .

############################
# STEP 2 build a small image
############################
FROM scratch
COPY --from=builder /dist/ca /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Command to run the executable
#ENTRYPOINT ["/ca"]
ENTRYPOINT ["/ca", "-new=true"]

EXPOSE 8300 8301