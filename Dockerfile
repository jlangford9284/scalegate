FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./

COPY cmd/ cmd/
COPY internal/ internal/

RUN go get github.com/prometheus/client_golang/prometheus/promhttp
RUN go mod tidy

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o scalegate cmd/scalegate/main.go

# Minimal base image for final stage
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /app/scalegate .
USER 65532:65532

ENTRYPOINT ["/scalegate"]
