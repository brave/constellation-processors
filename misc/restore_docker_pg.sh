#!/bin/sh
cat backup.sqldump | docker exec -i -e PGPASSWORD=password nested-star-aggregator_db_1 psql -U star postgres
