package io.kestra.plugin.opensearch;

import java.util.Map;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.GetRequest;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Retrieve a document from OpenSearch",
    description = "Fetches one document by index and id with optional routing and version match; returns the source map."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: opensearch_get
                namespace: company.team

                tasks:
                  - id: get
                    type: io.kestra.plugin.opensearch.Get
                    connection:
                      hosts:
                        - "http://localhost:9200"
                    index: "my_index"
                    key: "my_id"
                    docVersion: 1
                """
        )
    }
)
public class Get extends AbstractTask implements RunnableTask<Get.Output> {
    @Schema(
        title = "Target OpenSearch index"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> index;

    @Schema(
        title = "Document id"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> key;

    @Schema(
        title = "Expected document version",
        description = "Request fails if the cluster version differs; use for optimistic concurrency."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Long> docVersion;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        try (RestClientTransport transport = this.connection.client(runContext)) {
            OpenSearchClient client = new OpenSearchClient(transport);
            String index = runContext.render(this.index).as(String.class).orElseThrow();
            String key = runContext.render(this.key).as(String.class).orElseThrow();

            var request = new GetRequest.Builder();
            request.index(index).id(key);

            if (this.docVersion != null) {
                request.version(runContext.render(this.docVersion).as(Long.class).orElseThrow());
            }

            if (this.routing != null) {
                request.routing(runContext.render(this.routing).as(String.class).orElseThrow());
            }

            if (this.routing != null) {
                request.routing(runContext.render(this.routing).as(String.class).orElseThrow());
            }

            GetResponse<Map> response = client.get(request.build(), Map.class);
            logger.debug("Getting doc: {}", request);

            return Output.builder()
                .row(response.source())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private Map<String, Object> row;
    }
}
