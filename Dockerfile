# Build step
FROM golang:1.10 as builder

# Prerequisites
ADD . $GOPATH/src/AIflow/hdfs
WORKDIR $GOPATH/src/AIflow/hdfs

RUN CGO_ENABLED=0 go build -o hdfscp main.go \
  && cp hdfscp / 

# Final Step
FROM alpine:3.7
COPY --from=builder /hdfscp .