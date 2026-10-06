# syntax=docker/dockerfile:1.7

# ---- build stage: compiled once on the build platform, cross compiled per target ----
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0
# Dependencies first: this layer is cached until go.mod/go.sum change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG TARGETOS TARGETARCH
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    for b in api worker migrate; do \
      GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/$b ./cmd/$b || exit 1; \
    done

# ---- runtime stages: distroless static, non-root, no shell, no package manager ----
FROM gcr.io/distroless/static-debian12:nonroot AS api
COPY --from=build /out/api /app/api
USER nonroot:nonroot
EXPOSE 8080 9090
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 CMD ["/app/api", "healthcheck"]
ENTRYPOINT ["/app/api"]

FROM gcr.io/distroless/static-debian12:nonroot AS worker
COPY --from=build /out/worker /app/worker
USER nonroot:nonroot
EXPOSE 9090
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 CMD ["/app/worker", "healthcheck"]
ENTRYPOINT ["/app/worker"]

FROM gcr.io/distroless/static-debian12:nonroot AS migrate
COPY --from=build /out/migrate /app/migrate
USER nonroot:nonroot
ENTRYPOINT ["/app/migrate"]
