FROM minio/minio:RELEASE.2024-06-13T22-53-53Z
EXPOSE 9000 9001
# Volumen para datos
VOLUME ["/data"]
# Arrancamos MinIO con consola
ENTRYPOINT [ "minio" ]
CMD ["server", "/data", "--address",":9000","--console-address", ":9001"]