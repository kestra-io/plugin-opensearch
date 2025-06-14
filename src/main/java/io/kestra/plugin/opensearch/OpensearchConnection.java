package io.kestra.plugin.opensearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.SuperBuilder;
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

import java.net.URI;
import java.util.List;
import javax.net.ssl.SSLContext;
import jakarta.validation.constraints.NotNull;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.transport.rest_client.RestClientTransport;

@SuperBuilder
@NoArgsConstructor
@Getter
public class OpensearchConnection {
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson(false);

    @Schema(
        title = "List of HTTP OpenSearch servers.",
        description = "Must be an URI like `https://opensearch.com:9200` with scheme and port."
    )
    @NotNull
    private Property<List<String>> hosts;

    @Schema(
        title = "Basic auth configuration."
    )
    @PluginProperty(dynamic = false)
    private BasicAuth basicAuth;

    @Schema(
        title = "List of HTTP headers to be send on every request.",
        description = "Must be a string with key value separated with `:`, ex: `Authorization: Token XYZ`."
    )
    private Property<List<String>> headers;

    @Schema(
        title = "Sets the path's prefix for every request used by the HTTP client.",
        description = "For example, if this is set to `/my/path`, then any client request will become `/my/path/` + endpoint.\n" +
            "In essence, every request's endpoint is prefixed by this `pathPrefix`.\n" +
            "The path prefix is useful for when OpenSearch is behind a proxy that provides a base path " +
            "or a proxy that requires all paths to start with '/'; it is not intended for other purposes and " +
            "it should not be supplied in other scenarios."
    )
    private Property<String> pathPrefix;

    @Schema(
        title = "Whether the REST client should return any response containing at least one warning header as a failure."
    )
    private Property<Boolean> strictDeprecationMode;

    @Schema(
        title = "Trust all SSL CA certificates.",
        description = "Use this if the server is using a self signed SSL certificate."
    )
    private Property<Boolean> trustAllSsl;

    @SuperBuilder
    @NoArgsConstructor
    @Getter
    public static class BasicAuth {
        @Schema(
            title = "Basic auth username."
        )
        private Property<String> username;

        @Schema(
            title = "Basic auth password."
        )
        private Property<String> password;
    }

    RestClientTransport client(RunContext runContext) throws IllegalVariableEvaluationException {
        RestClientBuilder builder = RestClient
            .builder(this.httpHosts(runContext))
            .setHttpClientConfigCallback(httpClientBuilder -> {
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
            .map(s -> {
                URI uri = URI.create(s);
                return new HttpHost(uri.getScheme(), uri.getHost(), uri.getPort());
            })
            .toArray(HttpHost[]::new);
    }

    private Header[] defaultHeaders(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.headers).asList(String.class)
            .stream()
            .map(header -> {
                String[] nameAndValue = header.split(":");
                return new BasicHeader(nameAndValue[0], nameAndValue[1]);
            })
            .toArray(Header[]::new);
    }
}
