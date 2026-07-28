# syntax=docker/dockerfile:1

# ---- build stage ----
FROM golang:1.26-bookworm AS build
WORKDIR /src

# Cache modules first.
COPY go.mod ./
COPY go.su[m] ./
RUN go mod download

COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -buildid= -X leser/internal/buildinfo.Version=${VERSION} -X leser/internal/buildinfo.Commit=${COMMIT}" \
    -o /out/leser ./cmd/leser

# ---- runtime stage ----
# distroless static: no shell, no package manager, nonroot by default.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/leser /usr/local/bin/leser

EXPOSE 8080
VOLUME ["/data"]
ENV LESER_DATA_DIR=/data \
    LESER_LISTEN=:8080

# healthcheck hits the liveness endpoint (no shell, so exec form of the binary is
# used indirectly by orchestrators; distroless has no curl — rely on /healthz via
# the platform's HTTP probe instead).
ENTRYPOINT ["/usr/local/bin/leser"]
CMD ["serve"]
