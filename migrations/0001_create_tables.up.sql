CREATE TABLE IF NOT EXISTS collectors (
    collector_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    project_name VARCHAR(255) NOT NULL,
    last_fetch_timestamp TIMESTAMP NOT NULL DEFAULT GETDATE(),
    last_request_timestamp TIMESTAMP NOT NULL DEFAULT GETDATE()
);

CREATE TABLE IF NOT EXISTS bgp_dumps (
    bgp_dump_id SERIAL PRIMARY KEY,
    collector_name VARCHAR(255) NOT NULL,
    url TEXT NOT NULL,
    dump_type SMALLINT NOT NULL,
    duration INTERVAL,
    timestamp TIMESTAMP NOT NULL,
    first_fetch_timestamp TIMESTAMP DEFAULT NOT NULL GETDATE(),
    last_fetch_timestamp TIMESTAMP DEFAULT NOT NULL GETDATE(),
    CONSTRAINT unique_bgp_dump UNIQUE (collector_name, url)
);