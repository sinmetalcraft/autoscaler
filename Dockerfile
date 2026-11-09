FROM gcr.io/distroless/static-debian13
COPY ./app /app
ENTRYPOINT ["/app"]