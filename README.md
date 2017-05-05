# Event Data Query API Server

<img src="doc/logo.png" align="right" style="float: right">

Query API service for Event Data. Ingests from another source (e.g. another Query API, Event Bus or archive) and allows a number of queries to be run. Backed by MongoDB.

This codebase is used internally in Crossref Event Data, but you can easily run it to replicate Event Data or a subset of it, to your own database.

Provided as a Docker image for deployment. Docker Compose is used for testing. If you're on a Mac, you may want to run this outside Docker because MongoDB's mmaped files aren't compatible with the Docker host volume mapping (see [here](https://hub.docker.com/_/mongo/)).

## Usage as a replica

Anyone can run this as a replica, against the Crossref Query API, or against ananother replica. 

 - `lein run server` - run the server
 - `lein run replicate-continuous` - run automatic continuous replication from another Query API instance, from now onward
 - `lein run replicate-backfill-days «days»` - backfill from a number of days in the past
 - `lein run add-indexes` - one off, ensure that all indexes are present

If you want to run a standard setup you should run `server` and `replicate`, which will each run and keep running. The first time you run `replicate-continuous`, or if there has been an outage, you should run `replicate-backfill-days` to catch up.

Replication mode makes two types of queries to the upstream Query API: one for getting newly occurring Events, and one for getting newly updated Events (which may have originally occurred at any point in time). These queries are supplied in the form of a templated URL. If you want to update all of the available data you can leave the defaults. If you only want to replicate a subset of the data, e.g. for a given prefix or source id, you can supply a custom URL with a filter.

Replication occurs according to an internal schedule at 5am every day, UTC.

The default values are (noting the `%1$s` string substitution)

    REPLICA_COLLECTED_URL=https://query.eventdata.crossref.org/events?filter=from-collected-date:%1$s&cursor=%2$s&rows=10000
    REPLICA_UPDATED_URL=https://query.eventdata.crossref.org/events?from-updated-date:%1$s&cursor=%2$s&rows=10000

## Usage internally

The following methods are only for Crossref internal use as they depend on access-controlled internal resources.

 - `lein run server` - run the server
 - `lein run queue-continuous` - run automatic continuous replication via an ActiveMQ Queue
 - `lein run bus-backfill-days «days»` - backfill from a number of days in the past from the Event Bus archive
 - `lein run add-indexes` - one off, ensure that all indexes are present

### Source Whitelists

Because we may recieve data for more sources than we wish to store, the whitelist can be provided. This should be the name of a Crossref Artifact, e.g. `crossref-sourcelist`. The whitelist is applied on ingestion, so data must be backfilled if it was discarded due to a previous value.

### Demo commands

Server

    docker-compose -f docker-compose.yml run -w /usr/src/app --service-ports test lein run server

To run tests

    docker-compose -f docker-compose.yml run -w /usr/src/app test lein test

## Configuration

In all cases:

| Environment variable | Description              |
|----------------------|--------------------------|
| `MONGODB_URI`        | Connection URI for Mongo |

Running server:

| Environment variable | Description                         |
|----------------------|-------------------------------------|
| `PORT`               | Port to listen on                   |


Running as a replica:

| Environment variable    | Description                                                                                         |
|-------------------------|-----------------------------------------------------------------------------------------------------|
| `REPLICA_COLLECTED_URL` | Templated URL, described above. %1$s is start collection date, %2$ is cursor. Optional with default.|
| `REPLICA_UPDATED_URL`   | Templated URL, described above. %1$s is start update date, %2$ is cursor. Optional with default.    |

Running within Crossref:

| Environment variable      | Description                                                    |
|---------------------------|----------------------------------------------------------------|
| `WHITELIST_ARTIFACT_NAME` | Name of Artifact used for source whitelist. Optional.     |
| `STATUS_SERVICE`          | Public URL of the Status service. Optional.                    |
| `EVENT_BUS_BASE`          | Event Bus URL base. Optional.                                  |
| `ARTIFACT_BASE`           | Public URL of Artifact registry. Optional.                     |
| `JWT_TOKEN`               | JWT Token for authenticating with Bus. Optional.               |
| `TERMS`                   | A Terms URL to be associated with each event. Optional.        |
| `ACTIVEMQ_USERNAME`       | ActiveMQ credentials for ingestion queue.                      |
| `ACTIVEMQ_PASSWORD`       | ActiveMQ credentials for ingestion queue.                      |
| `ACTIVEMQ_URL`            | ActiveMQ connection URL for ingestion queue.                   |
| `ACTIVEMQ_QUEUE`          | Name of queue to ingest.                                       |



