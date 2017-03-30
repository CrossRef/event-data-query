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

To push a demo event for indexing:

    curl --verbose --header "Content-Type: application/json" --data  '{"obj_id":"https:\/\/doi.org\/10.1017\/s0963180100005168","source_token":"d9c55bad-73db-4860-be18-520d3891b01f","occurred_at":"2017-03-13T10:10:38.467Z","subj_id":"http:\/\/philpapers.org\/rec\/ANNAAS","id":"00003c22-1571-4bd3-924b-0438f6f7ff54","action":"add","subj":{"pid":"http:\/\/philpapers.org\/rec\/ANNAAS","work-type":"webpage","url":"http:\/\/philpapers.org\/rec\/ANNAAS"},"source_id":"test","obj":{"pid":"https:\/\/doi.org\/10.1017\/s0963180100005168","url":"https:\/\/doi.org\/10.1017\/s0963180100005168"},"timestamp":"2017-03-13T10:11:19.659Z","evidence-record":"https:\/\/evidence.eventdata.crossref.org\/evidence\/20170313e86bef03-4556-4ecc-8401-0e71af4d0bb6","relation_type_id":"mentions"}' -H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyIxIjoiMSIsInN1YiI6Indpa2lwZWRpYSJ9.w7zV2vtKNzrNDfgr9dfRpv6XYnspILRli_V5vd1J29Q" http://localhost:8100/events


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
| `JWT_SECRETS`        | JWT Secrets for push                |

