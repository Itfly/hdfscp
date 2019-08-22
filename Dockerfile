# Build step
FROM golang:1.12 as builder

# We want to populate the module cache based on the go.{mod,sum} files. 
RUN mkdir /hdfscp
WORKDIR /hdfscp
COPY go.mod .
COPY go.sum .

# Get dependencies
RUN go mod download

# copy the source code
COPY . .

RUN CGO_ENABLED=0 go build -o hdfscp

# Final Step
FROM alpine:3.7
COPY --from=builder /hdfscp/hdfscp .
