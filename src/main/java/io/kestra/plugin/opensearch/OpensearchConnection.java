package io.kestra.plugin.opensearch;

import java.net.URI;
import java.util.List;

import javax.net.ssl.SSLContext;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.auth.CredentialsProviderBuilder;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.ssl.TrustStrategy;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.transport.rest_client.RestClientTransport;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@NoArgsConstructor
@Getter
public class OpensearchConnection {
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson(false);

    @Schema(
        title = "OpenSearch HTTP endpoints",
        description = "One or more host URLs with scheme and port, e.g. `https://opensearch.com:9200`; all are used for load-balancing/failover."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<String>> hosts;

    @Schema(
        title = "Basic auth configuration"
    )
    @PluginProperty(dynamic = false, group = "advanced")
    private BasicAuth basicAuth;

    @Schema(
        title = "Additional HTTP headers",
        description = "Each entry is `Key:Value`, e.g. `Authorization: Token XYZ`; rendered per request."
    )
    @PluginProperty(group = "advanced")
    private Property<List<String>> headers;

    @Schema(
        title = "Path prefix for every request",
        description = "Prepends `/my/path` to all endpoints when OpenSearch is behind a proxy enforcing a base path; leave unset otherwise."
    )
    @PluginProperty(group = "advanced")
    private Property<String> pathPrefix;

    @Schema(
        title = "Fail on warning headers",
        description = "If true, any response containing an OpenSearch warning header is treated as a failure; defaults to server/client behavior."
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> strictDeprecationMode;

    @Schema(
        title = "Trust all SSL certificates",
        description = "Disables TLS verification for self-signed clusters; insecure, use only in trusted networks."
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> trustAllSsl;

    @SuperBuilder
    @NoArgsConstructor
    @Getter
    public static class BasicAuth {
        @Schema(
            title = "Basic auth username"
        )
        @PluginProperty(secret = true, group = "connection")
        private Property<String> username;

        @Schema(
            title = "Basic auth password"
        )
        @PluginProperty(secret = true, group = "connection")
        private Property<String> password;
    }

    RestClientTransport client(RunContext runContext) throws IllegalVariableEvaluationException {
        RestClientBuilder builder = RestClient
            .builder(this.httpHosts(runContext))
            .setHttpClientConfigCallback(httpClientBuilder ->
            {
                httpClientBuilder = this.httpAsyncClientBuilder(runContext);
                return httpClientBuilder;
            });

        if (this.getHeaders() != null) {
            builder.setDefaultHeaders(this.defaultHeaders(runContext));
        }

        if (this.getPathPrefix() != null) {
            builder.setPathPrefix(runContext.render(this.pathPrefix).as(String.class).orElseThrow());
        }

        if (this.getStrictDeprecationMode() != null) {
            builder.setStrictDeprecationMode(runContext.render(this.getStrictDeprecationMode()).as(Boolean.class).orElseThrow());
        }

        return new RestClientTransport(builder.build(), new JacksonJsonpMapper(MAPPER));
    }

    @SneakyThrows
    private HttpAsyncClientBuilder httpAsyncClientBuilder(RunContext runContext) {
        HttpAsyncClientBuilder builder = HttpAsyncClientBuilder.create();

        builder.setUserAgent("Kestra/" + runContext.version());

        if (basicAuth != null) {
            final CredentialsProvider basicCredential = CredentialsProviderBuilder.create()
                .add(
                    new AuthScope(null, -1),
                    runContext.render(this.basicAuth.username).as(String.class).orElseThrow(),
                    runContext.render(this.basicAuth.password).as(String.class).map(String::toCharArray).orElse(null)
                )
                .build();

            builder.setDefaultCredentialsProvider(basicCredential);
        }

        if (Boolean.TRUE.equals(runContext.render(trustAllSsl).as(Boolean.class).orElse(false))) {
            SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
            sslContextBuilder.loadTrustMaterial(null, (TrustStrategy) (chain, authType) -> true);
            SSLContext sslContext = sslContextBuilder.build();

            TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                .setSslContext(sslContext)
                .setHostnameVerifier(new NoopHostnameVerifier())
                .buildAsync();

            PoolingAsyncClientConnectionManager cm = PoolingAsyncClientConnectionManagerBuilder.create()
                .setTlsStrategy(tlsStrategy)
                .build();

            builder.setConnectionManager(cm);
        }

        return builder;
    }

    private HttpHost[] httpHosts(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.hosts).asList(String.class)
            .stream()
            .map(s ->
            {
                URI uri = URI.create(s);
                return new HttpHost(uri.getScheme(), uri.getHost(), uri.getPort());
            })
            .toArray(HttpHost[]::new);
    }

    private Header[] defaultHeaders(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.headers).asList(String.class)
            .stream()
            .map(header ->
            {
                String[] nameAndValue = header.split(":");
                return new BasicHeader(nameAndValue[0], nameAndValue[1]);
            })
            .toArray(Header[]::new);
    }
}
