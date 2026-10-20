package io.kestra.plugin.opensearch;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.plugin.opensearch.shared.OpensearchConnection;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class GetValidationTest {
    @Inject
    private ModelValidator modelValidator;

    @Test
    void shouldAllowGetWithoutDocVersion() {
        Get task = Get.builder()
            .id("get")
            .type("io.kestra.plugin.opensearch.Get")
            .connection(
                OpensearchConnection.builder()
                    .hosts(Property.ofValue(List.of("http://localhost:9200")))
                    .build()
            )
            .index(Property.ofValue("my_index"))
            .key(Property.ofValue("my_id"))
            .build();

        assertThat(modelValidator.isValid(task)).isEmpty();
    }
}
