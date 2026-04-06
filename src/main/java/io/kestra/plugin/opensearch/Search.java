package io.kestra.plugin.opensearch;

import java.io.*;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute an OpenSearch search",
    description = "Runs a search request and either returns rows, the first row, or stores results to Internal Storage; default fetchType is FETCH."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: opensearch_search
                namespace: company.team

                tasks:
                  - id: search
                    type: io.kestra.plugin.opensearch.Search
                    connection:
                      hosts:
                        - "http://localhost:9200"
                    indexes:
                      - "my_index"
                    request:
                      query:
                        term:
                          name:
                            value: 'john'
                """
        )
    }
)
public class Search extends AbstractSearch implements RunnableTask<Search.Output> {
    @Schema(
        title = "Result handling strategy",
        description = "FETCH returns all rows, FETCH_ONE returns the first row, STORE saves rows to Internal Storage, NONE skips output; defaults to FETCH."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (RestClientTransport transport = this.connection.client(runContext)) {
            OpenSearchClient client = new OpenSearchClient(transport);
            // build request
            SearchRequest.Builder request = this.request(runContext, transport);
            logger.debug("Starting query: {}", request);

            SearchResponse<Map> searchResponse = client.search(request.build(), Map.class);

            Output.OutputBuilder outputBuilder = Output.builder();

            switch (runContext.render(fetchType).as(FetchType.class).orElseThrow()) {
                case FETCH:
                    Pair<List<Map<String, Object>>, Integer> fetch = this.fetch(searchResponse);
                    outputBuilder
                        .rows(fetch.getLeft())
                        .size(fetch.getRight());
                    break;

                case FETCH_ONE:
                    var o = this.fetchOne(searchResponse);

                    outputBuilder
                        .row(o)
                        .size(o != null ? 1 : 0);
                    break;

                case STORE:
                    Pair<URI, Long> store = this.store(runContext, searchResponse);
                    outputBuilder
                        .uri(store.getLeft())
                        .size(store.getRight().intValue());
                    break;
            }

            // metrics
            runContext.metric(Counter.of("requests.count", 1));
            runContext.metric(Counter.of("records", searchResponse.hits().hits().size()));
            runContext.metric(Timer.of("requests.duration", Duration.ofNanos(searchResponse.took())));

            // outputs
            return outputBuilder
                .total(searchResponse.hits().total().value())
                .build();
        }
    }

    protected Pair<URI, Long> store(RunContext runContext, SearchResponse<Map> searchResponse) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
            Flux<Map> hitFlux = Flux.fromIterable(searchResponse.hits().hits()).map(hit -> hit.source());
            Long count = FileSerde.writeAll(output, hitFlux).block();

            return Pair.of(
                runContext.storage().putFile(tempFile),
                count
            );
        }
    }

    protected Pair<List<Map<String, Object>>, Integer> fetch(SearchResponse<Map> searchResponse) {
        List<Map<String, Object>> result = new ArrayList<>();

        searchResponse.hits().hits()
            .forEach(throwConsumer(docs -> result.add(docs.source())));

        return Pair.of(result, searchResponse.hits().hits().size());
    }

    protected Map<String, Object> fetchOne(SearchResponse<Map> searchResponse) {
        if (searchResponse.hits().hits().isEmpty()) {
            return null;
        }

        return searchResponse.hits().hits().getFirst().source();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of rows fetched"
        )
        private Integer size;

        @Schema(
            title = "Total hits without pagination"
        )
        private Long total;

        @Schema(
            title = "All fetched rows",
            description = "Populated when fetchType is FETCH."
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "First row fetched",
            description = "Populated when fetchType is FETCH_ONE."
        )
        private Map<String, Object> row;

        @Schema(
            title = "URI of stored data",
            description = "Populated when fetchType is STORE."
        )
        private URI uri;
    }
}
