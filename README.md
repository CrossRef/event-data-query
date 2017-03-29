# Event Data Query API Server

<img src="doc/logo.png" align="right" style="float: right">

Query service for Event Data. Ingests from another source (e.g. another Query API, Event Bus or archive) and allows a number of queries to be run. 

Backed by MongoDB.

## Usage

run with the following command:
 - `lein run server` - run the server
 - `lein run ingest-yesterday` - ingest yesterday's archive, unless already done
 - `lein run ingest-range` - ingest date range between start and end date inclusive, unless already done
 - `lein run reingest-range` - re-ingest date range between start and end date inclusive

Provided as a Docker image with Docker Compose file for testing. If you're on a mac, you may want to run this outside Docker because MongoDB's mmaped files aren't compatible with the Docker host volume mapping (see [here](https://hub.docker.com/_/mongo/)).


### Demo commands

Ingest

    docker-compose -f docker-compose.yml run -w /usr/src/app test lein run ingest-yesterday

Server

    docker-compose -f docker-compose.yml run -w /usr/src/app --service-ports test lein run server


To run a demo:

    docker-compose -f docker-compose.yml run -w /usr/src/app -p "8100:8100" test lein run

To run tests

    docker-compose -f docker-compose.yml run -w /usr/src/app test lein test

## Configuration

The `SOURCE_QUERY` config parameter is a templated string which accepts a `YYYY-MM-DD` date. It could be:

For all events:

    https://query.eventdata.crossref.org/occurred/%s/events.json

For your prefix:

    https://query.eventdata.crosrsef.org/occurred/%s/prefixes/10.5555/events.json

etc

| Environment variable | Description                         |
|----------------------|-------------------------------------|
| `PORT`               | Port to listen on                   |
| `STATUS_SERVICE`     | Public URL of the Status service    |
| `EVENT_BUS_BASE`     | Event Bus URL base                  |
| `SERVICE_BASE`       | Public URL base of this service, not including slash. |
| `ARTIFACT_BASE`      | Public URL of Artifact registry     |
| `SOURCE_QUERY`       | Template source query URL.          |
| `MONGODB_URI`        | Connection URI for Mongo            |