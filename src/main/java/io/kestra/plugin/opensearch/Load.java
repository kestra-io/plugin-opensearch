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
    title = "Bulk load documents into OpenSearch using an Internal Storage file."
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
        title = "The OpenSearch index."
    )
    @NotNull
    private Property<String> index;

    @Schema(
        title = "Sets the type of operation to perform."
    )
    private Property<OpType> opType;

    @Schema(
        title = "Use this key as id."
    )
    private Property<String> idKey;

    @Schema(
        title = "Remove idKey from the final document."
    )
    @Builder.Default
    private Property<Boolean> removeIdKey = Property.of(true);

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
