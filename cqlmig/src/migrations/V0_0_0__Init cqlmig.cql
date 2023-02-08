
CREATE KEYSPACE IF NOT EXISTS migration WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3 };

CREATE TABLE IF NOT EXISTS migration.migrations
(
    version     text PRIMARY KEY,
    description text,
    applied     timestamp,
    -- when lading bytes or files else 0
    shasum      text,
    status      int,
    status_text text
)
