# Changelog
All notable changes to this project will be documented in this file.

## 0.2.3 - 2017-03-08

Added /ids endpoints for quick integrity checks.

### Added

 - IDs endpoints:
   - `/v1/events/ids`
   - `/v1/events/edited/ids`
   - `/v1/events/deleted/ids`
   - `/v1/events/experimental/ids`
   - `/v1/events/scholix/ids`
   - `/v1/events/distinct/ids`
 - Each endpoint has the 'event-id-list` type, except Scholix which is 'link-package-id'.

### Changed
 - Each endpoint now has a correct `message-type` value.
 - The key (under `message`) is now correct, dependent on the endpoint.
 - v1/events/scholix message-type is `link-packages` and key is `link-package`.
 - Docker image now includes dependencies. It was taking too long to start up in production.

### Removed

Nothing.

## 0.2.0-SNAPSHOT - 2018-02-28

Upgrade from ElasticSearch 5.x to 6.x and performance fixes for updated and deleted Events.

### Added

- First CHANGELOG.md.
- Config QUERY_DEPLOYMENT config key to namespace ElasticSearch indexes.
- Set the refresh_interval for the duration of an index rebuild from archive for insert performance.
- Added indexed fields in Elastic:
 - subj-ra
 - obj-ra
 - subj-content-type
 - obj-content-type
- End-to-end test in addition to unit tests.
- Lookup work metadata (RA, content type) and store in ElasticSearch cache. This is only done for the Scholix endpoint.

### Changed
- Upgrading from ElasticSearch 5.x to 6.x. This means one type per collection, so re-jigging types.
- Indexes split out into six:
  1. live
  2. latest
  3. updated
  4. deleted
  5. experimental
  6. scholix

- Routes for Events, query syntax and API split out:
  - `/events` - All non-deleted, non-experimental.
  - `/events/deleted` - Non-experimental Events that have been deleted.
  - `/events/edited` - Non-experimental Events that have been edited.
  - `/events/experimental` - Non-production experimental data.
  - `/events/scholix` - Events that connect DataCite and Crossref DOIs, for data citation.
- Change to how deleted Events are retrieved:
  - These are now available under the `/events/deleted` route, and are removed from the `/events` route.
  - Deleted Events can be found at the `/events/deleted/«id»` route. 
  - The 'include-deleted' query parameter has been removed.
- All URLs now in /v1 namespace.
- List of acceptable HTTP response codes from the _batch insert are now scoped to action.


### Removed
- Removed /special/alternative-ids-check route.
- Removed some query parameters, as these are in new routes:
  - experimental removed from filters
  - updated-date removed from time-based facet
- Removed some indexed fields, as there are other methods to query them:
  - experimental
