package io.kestra.plugin.opensearch;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.transport.rest_client.RestClientTransport;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.opensearch.shared.BulkService;

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
public abstract class AbstractLoad extends AbstractTask implements RunnableTask<AbstractLoad.Output> {
    @Schema(
        title = "Source file in Internal Storage",
        description = "Path to Kestra internal storage object containing line-delimited JSON or ION records."
    )
    @NotNull
    @PluginProperty(internalStorageURI = true, group = "main")
    private Property<String> from;

    @Schema(
        title = "Bulk chunk size",
        description = "Number of operations per bulk request; defaults to 1000."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Integer> chunk = Property.ofValue(1000);

    protected abstract Flux<BulkOperation> source(RunContext runContext, InputStream inputStream) throws IllegalVariableEvaluationException, IOException;

    @Override
    public Output run(RunContext runContext) throws Exception {
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        try (
            RestClientTransport transport = this.connection.client(runContext);
            InputStream inputStream = new BufferedInputStream(runContext.storage().getFile(from), FileSerde.BUFFER_SIZE)
        ) {
            var chunkRendered = runContext.render(this.chunk).as(Integer.class).orElseThrow();

            long size = BulkService.executeBulk(runContext, transport, this.source(runContext, inputStream), chunkRendered);

            return Output.builder()
                .size(size)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of records sent"
        )
        private Long size;
    }
}
