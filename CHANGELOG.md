# Changelog

All notable, user-facing changes to this plugin are documented here.

## [1.5.5] - Unreleased

Connection handling and the bulk-execution pipeline now come from the shared
`plugin-opensearch-lib` dependency. This changes two user-visible behaviors of the
`connection` block. Both are intentional (security hardening / configurability) but can
break existing flows on upgrade.

### Changed — behavior

- **`trustAllSsl` no longer disables hostname verification.** The previous in-repo
  connection installed a `NoopHostnameVerifier`; the shared connection keeps hostname
  verification enforced even when `trustAllSsl: true` (CWE-295 / CWE-297). Flows reaching a
  self-signed cluster **by IP**, or by a hostname **absent from the certificate SAN**, now
  fail with `SSLPeerUnverifiedException` / `SSLHandshakeException` where they previously
  connected.
  **Fix:** use a hostname present in the certificate SAN, or supply a trusted CA / trust store.

- **HTTP connect/response timeout defaults changed.** The connection now inherits
  RestClient's defaults — connect timeout `3 min → 1 s`, response timeout `none → 30 s`,
  automatic retries disabled. A `Load` / `Bulk` with the default `chunk` against a slow or
  remote cluster taking >30 s per request can now fail with a socket timeout.
  **Fix:** raise the limits with the new `connection.connectTimeout` /
  `connection.responseTimeout` properties.
