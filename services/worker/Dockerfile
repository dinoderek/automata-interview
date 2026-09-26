FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY services/worker ./services/worker
RUN go build -o /out/worker ./services/worker

FROM alpine:3.20
RUN adduser -D -u 10001 app
USER app
COPY --from=build /out/worker /usr/local/bin/worker

ENTRYPOINT ["/usr/local/bin/worker"]
