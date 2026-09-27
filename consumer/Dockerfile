FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/cdc-consumer .

FROM alpine:3.22

LABEL org.opencontainers.image.title="CDCCore" \
      org.opencontainers.image.description="Consumer and Replicator for Change Data Capture"

RUN apk add --no-cache ca-certificates wget
WORKDIR /app
COPY --from=build /out/cdc-consumer /usr/local/bin/cdc-consumer

EXPOSE 8080
ENTRYPOINT ["cdc-consumer"]
