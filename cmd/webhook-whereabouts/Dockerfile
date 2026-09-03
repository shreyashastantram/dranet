FROM golang:1.26 AS builder
WORKDIR /src
COPY . .
WORKDIR /src/cmd/webhook-whereabouts
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o /webhook-whereabouts .

FROM alpine:latest
COPY --from=builder /webhook-whereabouts /usr/local/bin/webhook-whereabouts
ENTRYPOINT ["/usr/local/bin/webhook-whereabouts"]
