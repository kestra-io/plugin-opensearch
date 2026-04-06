package io.kestra.plugin.opensearch;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;

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
public abstract class AbstractTask extends Task {
    @Schema(
        title = "Configure OpenSearch connection",
        description = "Hosts, auth headers, and TLS options reused by every task invocation."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected OpensearchConnection connection;

    @Schema(
        title = "Shard routing key",
        description = "Hashes routing using this value instead of the document id to colocate related records."
    )
    @PluginProperty(group = "advanced")
    protected Property<String> routing;
}
