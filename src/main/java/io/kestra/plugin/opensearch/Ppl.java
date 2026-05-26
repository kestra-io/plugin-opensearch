package io.kestra.plugin.opensearch;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.entity.EntityBuilder;
import org.apache.hc.core5.http.ContentType;
import org.opensearch.client.Response;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run an OpenSearch PPL query",
    description = """
        Runs a Piped Processing Language query against the OpenSearch `_plugins/_ppl` endpoint.

        PPL is OpenSearch's counterpart to Elasticsearch ES|QL. The two are not interchangeable. A query written for one engine will not run on the other, so dashboards that aggregate across both engines need engine-specific queries.

        Requires the optional opensearch-sql plugin on the cluster. The task fails if the plugin is missing.
        """
)
@Plugin(
    metrics = {
        @Metric(name = "requests.count", type = Counter.TYPE, description = "Number of PPL requests sent"),
        @Metric(name = "records", type = Counter.TYPE, unit = "records", description = "Number of records returned")
    },
    examples = {
        @Example(
            title = "Filter and aggregate with PPL.",
            full = true,
            code = """
                id: opensearch_ppl
                namespace: company.team

                tasks:
                  - id: ppl_query
                    type: io.kestra.plugin.opensearch.Ppl
                    connection:
                      hosts:
                        - "http://localhost:9200"
                    query: "source=accounts | where age > 30 | stats count() by gender"
                    fetchType: FETCH
                """
        )
    }
)
public class Ppl extends AbstractTask implements RunnableTask<Ppl.Output> {
    @Schema(
        title = "PPL query string",
        description = "PPL statement rendered at runtime."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> query;

    @Schema(
        title = "Maximum number of rows to return",
        description = "Sent as `fetch_size` in the request body. The cluster decides the effective cap."
    )
    @PluginProperty(group = "main")
    protected Property<Integer> fetchSize;

    @Schema(
        title = "Response format",
        description = "Sent as the `format` query-string parameter. Defaults to `JDBC`. `CSV` and `RAW` return text payloads."
    )
    @Builder.Default
    @PluginProperty(group = "main")
    protected Property<Format> format = Property.ofValue(Format.JDBC);

    @Schema(
        title = "Result handling mode",
        description = "Controls how query results are exposed. `FETCH` returns all rows. `FETCH_ONE` returns the first row (JDBC format only). `STORE` writes results to Kestra internal storage and returns a URI. `NONE` produces no output. For `CSV` and `RAW` formats, `FETCH` and `FETCH_ONE` return the full text payload in the `text` output; `STORE` saves it as a file."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        var rQuery = runContext.render(this.query).as(String.class).orElseThrow();
        var rFetchSize = runContext.render(this.fetchSize).as(Integer.class).orElse(null);
        var rFormat = runContext.render(this.format).as(Format.class).orElse(Format.JDBC);
        var rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.FETCH);

        try (RestClientTransport transport = this.connection.client(runContext)) {
            var request = new org.opensearch.client.Request("POST", "/_plugins/_ppl");
            request.addParameter("format", rFormat.name().toLowerCase());

            Map<String, Object> bodyMap = new HashMap<>();
            bodyMap.put("query", rQuery);
            if (rFetchSize != null) {
                bodyMap.put("fetch_size", rFetchSize);
            }
            var bodyJson = JacksonMapper.ofJson().writeValueAsString(bodyMap);

            request.setEntity(
                EntityBuilder
                    .create()
                    .setContentType(ContentType.APPLICATION_JSON)
                    .setText(bodyJson)
                    .build()
            );

            logger.debug("Starting PPL request: {}", bodyJson);

            Response response = transport.restClient().performRequest(request);

            response.getWarnings().forEach(logger::warn);

            var content = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);

            runContext.metric(Counter.of("requests.count", 1));

            return rFormat == Format.JDBC
                ? buildJdbcOutput(runContext, content, rFetchType)
                : buildTextOutput(runContext, content, rFormat, rFetchType);
        }
    }

    @SuppressWarnings("unchecked")
    private Output buildJdbcOutput(RunContext runContext, String content, FetchType fetchType) throws Exception {
        Map<String, Object> envelope = JacksonMapper.toMap(content);

        Long total = envelope.get("total") instanceof Number n ? n.longValue() : null;
        List<Map<String, Object>> schema = (List<Map<String, Object>>) envelope.getOrDefault("schema", List.of());
        List<List<Object>> datarows = (List<List<Object>>) envelope.getOrDefault("datarows", List.of());

        List<String> columns = schema.stream().map(s -> (String) s.get("name")).toList();
        List<Map<String, Object>> rows = datarows.stream()
            .map(values ->
            {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 0; i < columns.size() && i < values.size(); i++) {
                    row.put(columns.get(i), values.get(i));
                }
                return row;
            })
            .toList();

        Output.OutputBuilder builder = Output.builder().total(total);

        switch (fetchType) {
            case FETCH -> builder.rows(rows).size(rows.size());
            case FETCH_ONE -> builder.row(rows.isEmpty() ? null : rows.getFirst()).size(rows.isEmpty() ? 0 : 1);
            case STORE -> {
                File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                try (var output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
                    Long count = FileSerde.writeAll(output, Flux.fromIterable(rows)).block();
                    builder.uri(runContext.storage().putFile(tempFile)).size(count == null ? 0 : count.intValue());
                }
            }
            case NONE -> builder.size(rows.size());
        }

        runContext.metric(Counter.of("records", rows.size()));
        return builder.build();
    }

    private Output buildTextOutput(RunContext runContext, String content, Format format, FetchType fetchType) throws Exception {
        Output.OutputBuilder builder = Output.builder();

        switch (fetchType) {
            case FETCH, FETCH_ONE -> builder.text(content);
            case STORE -> {
                File tempFile = runContext.workingDir().createTempFile("." + format.name().toLowerCase()).toFile();
                Files.writeString(tempFile.toPath(), content, StandardCharsets.UTF_8);
                builder.uri(runContext.storage().putFile(tempFile));
            }
            case NONE -> {
            }
        }

        return builder.build();
    }

    public enum Format {
        JDBC,
        CSV,
        RAW
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Returned row count",
            description = "Number of rows included in outputs for the selected fetchType."
        )
        private Integer size;

        @Schema(
            title = "Total rows reported",
            description = "Total rows returned by the PPL response envelope."
        )
        private Long total;

        @Schema(
            title = "Fetched rows",
            description = "Set when fetchType is `FETCH` and format is `JDBC`. Contains all rows from the response."
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "First row",
            description = "Set when fetchType is `FETCH_ONE` and format is `JDBC`. Contains the first row only."
        )
        private Map<String, Object> row;

        @Schema(
            title = "Stored data URI",
            description = "Set when fetchType is `STORE`. Kestra internal storage path. Ion file for `JDBC`, text file for `CSV` and `RAW`."
        )
        private URI uri;

        @Schema(
            title = "Raw text payload",
            description = "Set when format is `CSV` or `RAW` and fetchType is `FETCH` or `FETCH_ONE`. Full response body as text."
        )
        private String text;
    }
}
