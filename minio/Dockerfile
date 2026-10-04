# MinIO no longer publishes images or binaries (Docker Hub denied, dl.min.io 410 Gone), so build the pinned source.
FROM golang:1.25-alpine AS build
RUN apk add --no-cache git
ARG MINIO_COMMIT=7aac2a2c5b7c882e68c1ce017d8256be2feea27f
RUN git clone https://github.com/minio/minio /src && cd /src && git checkout $MINIO_COMMIT \
    && CGO_ENABLED=0 go build -trimpath -o /minio .

FROM alpine:3.21
COPY --from=build /minio /usr/local/bin/minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
