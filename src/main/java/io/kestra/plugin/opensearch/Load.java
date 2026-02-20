package io.kestra.plugin.opensearch;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.opensearch.model.OpType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;
import jakarta.validation.constraints.NotNull;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import reactor.core.publisher.Flux;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Bulk load records from Internal Storage",
    description = "Reads line-delimited JSON/ION from Kestra internal storage and indexes documents in batches."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: opensearch_load
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: load
                    type: io.kestra.plugin.opensearch.Load
                    connection:
                      hosts:
                        - "http://localhost:9200"
                    from: "{{ inputs.file }}"
                    index: "my_index"
                """
        )
    }
)
public class Load extends AbstractLoad implements RunnableTask<Load.Output> {

    @Schema(
        title = "Target OpenSearch index"
    )
    @NotNull
    private Property<String> index;

    @Schema(
        title = "Bulk operation type",
        description = "Indexing op type; INDEX/CREATE supported, others rejected by client."
    )
    private Property<OpType> opType;

    @Schema(
        title = "Field to use as document id",
        description = "If set, uses this field value as `_id`; field is removed when `removeIdKey` is true."
    )
    private Property<String> idKey;

    @Schema(
        title = "Remove idKey from document",
        description = "Defaults to true; keep the id field in the document by setting to false."
    )
    @Builder.Default
    private Property<Boolean> removeIdKey = Property.ofValue(true);

    @SuppressWarnings("unchecked")
    @Override
    protected Flux<BulkOperation> source(RunContext runContext, BufferedReader inputStream) throws IllegalVariableEvaluationException, IOException {
        return FileSerde.readAll(inputStream)
            .map(throwFunction(o -> {
                Map<String, ?> values = (Map<String, ?>) o;

                var indexRequest = new IndexOperation.Builder<Map<String, ?>>();
                if (this.index != null) {
                    indexRequest.index(runContext.render(this.getIndex()).as(String.class).orElseThrow());
                }

                //FIXME
//                if (this.opType != null) {
//                    indexRequest.opType(this.opType.to());
//                }

                if (this.idKey != null) {
                    String idKey = runContext.render(this.idKey).as(String.class).orElseThrow();

                    indexRequest.id(values.get(idKey).toString());

                    if (Boolean.TRUE.equals(runContext.render(removeIdKey).as(Boolean.class).orElseThrow())) {
                        values.remove(idKey);
                    }
                }

                indexRequest.document(values);

                var bulkOperation = new BulkOperation.Builder();
                bulkOperation.index(indexRequest.build());
                return bulkOperation.build();
            }));
    }
}
