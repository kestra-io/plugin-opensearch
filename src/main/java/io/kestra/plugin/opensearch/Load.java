package io.kestra.plugin.opensearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.CreateOperation;
import org.opensearch.client.opensearch.core.bulk.DeleteOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.core.bulk.UpdateOperation;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.opensearch.model.OpType;

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
    title = "Bulk load records from Internal Storage",
    description = "Reads line-delimited JSON/ION from Kestra internal storage and sends the records in batches as `INDEX`, `CREATE`, `UPDATE` or `DELETE` operations, depending on `opType`."
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
        ),
        @Example(
            full = true,
            code = """
                id: opensearch_load_update
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: update
                    type: io.kestra.plugin.opensearch.Load
                    connection:
                      hosts:
                        - "http://localhost:9200"
                    from: "{{ inputs.file }}"
                    index: "my_index"
                    idKey: "id"
                    opType: UPDATE
                """
        )
    }
)
public class Load extends AbstractLoad implements RunnableTask<Load.Output> {

    @Schema(
        title = "Target OpenSearch index"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> index;

    @Schema(
        title = "Bulk operation type",
        description = """
            Bulk operation applied to each record: `INDEX` (default), `CREATE`, `UPDATE` (partial update, upserting the record if missing), or `DELETE`.
            `CREATE` fails the task if a document with the same id already exists.
            `UPDATE` and `DELETE` require `idKey`.
            The load is not atomic: records are validated while streaming, so a failure on a bad record (e.g. a missing `idKey` field) can leave earlier chunks already applied."""
    )
    @PluginProperty(group = "advanced")
    private Property<OpType> opType;

    @Schema(
        title = "Field to use as document id",
        description = "If set, uses this field value as `_id`; field is removed when `removeIdKey` is true. Required when `opType` is `UPDATE` or `DELETE`; optional otherwise (OpenSearch generates ids when unset)."
    )
    @PluginProperty(group = "connection")
    private Property<String> idKey;

    @Schema(
        title = "Remove idKey from document",
        description = "Defaults to true; keep the id field in the document by setting to false."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<Boolean> removeIdKey = Property.ofValue(true);

    @SuppressWarnings("unchecked")
    @Override
    protected Flux<BulkOperation> source(RunContext runContext, InputStream inputStream) throws IllegalVariableEvaluationException, IOException {
        var index = runContext.render(this.index).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`index` is required"));
        var opType = runContext.render(this.opType).as(OpType.class).orElse(OpType.INDEX);
        var idKey = runContext.render(this.idKey).as(String.class).orElse(null);
        var removeIdKey = runContext.render(this.removeIdKey).as(Boolean.class).orElse(true);

        if (idKey == null && (opType == OpType.UPDATE || opType == OpType.DELETE)) {
            throw new IllegalArgumentException("`idKey` is required when `opType` is " + opType);
        }

        return FileSerde.readAll(inputStream)
            .map(o ->
            {
                var values = (Map<String, ?>) o;

                String id = null;
                if (idKey != null) {
                    var idValue = values.get(idKey);
                    if (idValue == null) {
                        throw new IllegalArgumentException("Record is missing idKey '" + idKey + "'; required for opType " + opType);
                    }
                    id = idValue.toString();

                    if (removeIdKey) {
                        values.remove(idKey);
                    }
                }

                return operation(opType, index, id, values);
            });
    }

    private static BulkOperation operation(OpType opType, String index, String id, Map<String, ?> values) {
        var bulkOperation = new BulkOperation.Builder();

        switch (opType) {
            case INDEX -> bulkOperation.index(
                new IndexOperation.Builder<Map<String, ?>>()
                    .index(index)
                    .id(id)
                    .document(values)
                    .build()
            );
            case CREATE -> bulkOperation.create(
                new CreateOperation.Builder<Map<String, ?>>()
                    .index(index)
                    .id(id)
                    .document(values)
                    .build()
            );
            case UPDATE -> bulkOperation.update(
                new UpdateOperation.Builder<Map<String, ?>>()
                    .index(index)
                    .id(id)
                    .document(values)
                    .docAsUpsert(true)
                    .build()
            );
            case DELETE -> bulkOperation.delete(
                new DeleteOperation.Builder()
                    .index(index)
                    .id(id)
                    .build()
            );
        }

        return bulkOperation.build();
    }
}
