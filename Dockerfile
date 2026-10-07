FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY main.go ./

# Fetch dependencies and tidy
RUN go get github.com/prometheus/common@v0.45.0
RUN go mod tidy

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o scalegate main.go

# Minimal base image for final stage
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /app/scalegate .
USER 65532:65532

ENTRYPOINT ["/scalegate"]
