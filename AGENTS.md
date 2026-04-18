# Kestra OpenSearch Plugin

## What

- Provides plugin components under `io.kestra.plugin.opensearch`.
- Includes classes such as `Request`, `Load`, `Scroll`, `OpensearchConnection`.

## Why

- This plugin integrates Kestra with OpenSearch.
- It provides tasks that load, search, and manage data in OpenSearch clusters.

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
