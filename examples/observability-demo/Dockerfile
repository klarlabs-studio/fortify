# syntax=docker/dockerfile:1
# Build the sample app from the repo root so the local replace
# directive in examples/observability-demo/go.mod resolves to the
# parent module sources.
FROM golang:1.27-alpine@sha256:eb61c3873c97416fff0116553fff65d67a3549f72e11d9758849a689cdd8b523 AS build
WORKDIR /src
COPY --chown=0:0 . .
WORKDIR /src/examples/observability-demo
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download && \
    CGO_ENABLED=0 go build -trimpath -o /out/sample-app .

FROM gcr.io/distroless/static:nonroot@sha256:aafe993665bf6200db38a02c11a9351e782a84b6a45e7f33b6695638c88e3931
LABEL org.opencontainers.image.authors="Klarlabs <https://github.com/klarlabs-studio>"
COPY --from=build --chown=nonroot:nonroot /out/sample-app /sample-app
USER nonroot:nonroot
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=2s --retries=5 CMD ["/sample-app", "healthcheck"]
ENTRYPOINT ["/sample-app"]
