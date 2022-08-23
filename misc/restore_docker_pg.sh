#!/bin/sh
cat backup.sqldump | docker exec -i -e PGPASSWORD=password nested-star-processors_db_1 psql -U star postgres
