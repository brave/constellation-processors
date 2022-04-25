#!/bin/sh
docker exec -e PGPASSWORD=password nested-star-aggregator_db_1 pg_dump -a -U star postgres > backup.sqldump
