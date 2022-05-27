#!/bin/bash

docker build --tag bigdatafull .

docker rm bigdata-db
docker run --name bigdata-db -e POSTGRES_PASSWORD=testPassword -p 5433:5432 -d postgres

docker run -p 8888:8888 -i -t -v $(pwd)/src:/src --name tarea3 bigdatafull

# docker exec -it tarea3 /bin/bash
# psql -h host.docker.internal -p 5433 -U postgres
# psql -h 172.17.0.1 -p 5433 -U postgres

api-tradestation-crm-notification--per-cluster-tls