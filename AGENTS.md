# Kestra OpenSearch Plugin

## What

- Provides plugin components under `io.kestra.plugin.opensearch`.
- Includes classes such as `Request`, `Load`, `Scroll`, `OpensearchConnection`.

## Why

- What user problem does this solve? Teams need to load, search, and manage data in OpenSearch clusters from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps OpenSearch steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on OpenSearch.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `opensearch`

Infrastructure dependencies (Docker Compose services):

- `opensearch`

### Key Plugin Classes

- `io.kestra.plugin.opensearch.Bulk`
- `io.kestra.plugin.opensearch.Get`
- `io.kestra.plugin.opensearch.Load`
- `io.kestra.plugin.opensearch.Put`
- `io.kestra.plugin.opensearch.Request`
- `io.kestra.plugin.opensearch.Scroll`
- `io.kestra.plugin.opensearch.Search`

### Project Structure

```
plugin-opensearch/
├── src/main/java/io/kestra/plugin/opensearch/model/
├── src/test/java/io/kestra/plugin/opensearch/model/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
