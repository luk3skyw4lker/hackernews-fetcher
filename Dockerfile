# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags="-s -w -X github.com/luk3skyw4lker/hackernews-fetcher/internal/cli.Version=${VERSION}" \
      -o /out/hnfetcher ./cmd/hnfetcher

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hnfetcher /hnfetcher
EXPOSE 8080
USER nonroot:nonroot
# Distroless has no curl/wget: the binary probes itself.
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 CMD ["/hnfetcher", "healthcheck"]
ENTRYPOINT ["/hnfetcher"]
CMD ["serve"]
