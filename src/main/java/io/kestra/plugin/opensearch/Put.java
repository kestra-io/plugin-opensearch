package io.kestra.plugin.opensearch;

import java.util.Map;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Result;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.opensearch.model.OpType;
import io.kestra.plugin.opensearch.model.RefreshPolicy;
import io.kestra.plugin.opensearch.model.XContentType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Index a single OpenSearch document",
    description = "Writes one document with optional custom id, opType, routing, and refresh policy; accepts JSON string or Map payload."
)
@Plugin(
    examples = {
        @Example(
            title = "Put a document with a Map.",
            full = true,
            code = """
                id: opensearch_put
                namespace: company.team

                tasks:
                  - id: put
                    type: io.kestra.plugin.opensearch.Put
                    connection:
                      hosts:
                        - "http://localhost:9200"
                    index: "my_index"
                    key: "my_id"
                    value:
                      name: "John Doe"
                      city: "Paris"
                """
        ),
        @Example(
            title = "Put a document from a JSON string.",
            full = true,
            code = """
                id: opensearch_put
                namespace: company.team

                inputs:
                  - id: value
                    type: JSON
                    defaults: {"name": "John Doe", "city": "Paris"}

                tasks:
                  - id: put
                    type: io.kestra.plugin.opensearch.Put
                    connection:
                      hosts:
                        - "http://localhost:9200"
                    index: "my_index"
                    key: "my_id"
                    value: "{{ inputs.value }}"
                """
        ),
    }
)
public class Put extends AbstractTask implements RunnableTask<Put.Output> {
    private static ObjectMapper MAPPER = JacksonMapper.ofJson();

    @Schema(
        title = "Target OpenSearch index"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> index;

    @Schema(
        title = "Operation type",
        description = "INDEX or CREATE are supported; others are rejected."
    )
    @PluginProperty(group = "advanced")
    private Property<OpType> opType;

    @Schema(
        title = "Document id"
    )
    @PluginProperty(group = "connection")
    private Property<String> key;

    @Schema(
        title = "Document body",
        description = "String rendered then parsed using `contentType`, or a Map rendered and sent as JSON."
    )
    @PluginProperty(dynamic = true, group = "advanced")
    private Object value;

    @Schema(
        title = "Refresh policy",
        description = "IMMEDIATE forces refresh, WAIT_UNTIL waits for refresh, NONE (default) leaves refresh to cluster."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<RefreshPolicy> refreshPolicy = Property.ofValue(RefreshPolicy.NONE);

    @Schema(
        title = "Payload content type",
        description = "Format used when `value` is a string; defaults to JSON."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<XContentType> contentType = Property.ofValue(XContentType.JSON);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        try (RestClientTransport transport = this.connection.client(runContext)) {
            OpenSearchClient client = new OpenSearchClient(transport);
            String index = runContext.render(this.index).as(String.class).orElseThrow();
            String key = runContext.render(this.key).as(String.class).orElse(null);

            var request = new IndexRequest.Builder<Map>();
            request.index(index);

            this.source(runContext, request);

            if (key != null) {
                request.id(key);
            }

            if (this.opType != null) {
                request.opType(runContext.render(this.opType).as(OpType.class).orElseThrow().to());
            }

            if (this.refreshPolicy != null) {
                request.refresh(runContext.render(this.refreshPolicy).as(RefreshPolicy.class).orElseThrow().to());
            }

            if (this.routing != null) {
                request.routing(runContext.render(this.routing).as(String.class).orElseThrow());
            }

            logger.debug("Putting doc: {}", request);

            IndexResponse response = client.index(request.build());

            return Output.builder()
                .id(response.id())
                .result(response.result())
                .version(response.version())
                .build();
        }
    }

    @SuppressWarnings("unchecked")
    private void source(RunContext runContext, IndexRequest.Builder<Map> request) throws IllegalVariableEvaluationException, JsonProcessingException {
        if (this.value instanceof String valueStr) {
            Map<?, ?> document = MAPPER.readValue(runContext.render(valueStr), Map.class);
            // FIXME contentType
            request.document(document);
        } else if (this.value instanceof Map valueMap) {
            request.document(runContext.render(valueMap));
        } else {
            throw new IllegalVariableEvaluationException("Invalid value type '" + this.value.getClass() + "'");
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Document id written"
        )
        private String id;

        @Schema(
            title = "Result of the write"
        )
        private Result result;

        @Schema(
            title = "Document version"
        )
        private Long version;
    }
}
