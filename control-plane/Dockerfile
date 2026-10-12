# syntax=docker/dockerfile:1

# ---- build: compile both binaries -------------------------------------------
    FROM golang:1.24-bookworm AS build
    WORKDIR /src
    
    # Dependencies first, so this layer is cached until go.mod/go.sum change.
    COPY go.mod go.sum ./
    RUN go mod download
    
    COPY . .
    # CGO_ENABLED=0 → fully static binaries that run on a distroless base.
    # -trimpath and -s -w strip local paths and debug info → smaller, reproducible.
    RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/control-plane ./cmd/control-plane \
     && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker
    
    # ---- test: the full suite, run by `docker compose --profile test run test` ---
    # Based on the build stage so it has the Go toolchain and gcc (needed by -race).
    FROM build AS test
    CMD ["go", "test", "-race", "-count=1", "./..."]
    
    # ---- runtime: just the two binaries ------------------------------------------
    # distroless/static: no shell, no package manager, runs as non-root.
    FROM gcr.io/distroless/static-debian12:nonroot AS runtime
    COPY --from=build /out/control-plane /out/worker /usr/local/bin/
    EXPOSE 8080
    # Default program; compose overrides the entrypoint for the worker.
    ENTRYPOINT ["control-plane"]
    