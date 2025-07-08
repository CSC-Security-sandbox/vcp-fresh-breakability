FROM alpine:3.21.0
COPY core/build/linux/bin/vcp-db-migrate /vcp-db-migrate
COPY database/postgres/migrations/core/pre/*.sql /migrations/core/pre/
COPY database/postgres/migrations/core/post/*.sql /migrations/core/post/




