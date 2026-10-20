package io.kestra.plugin.opensearch;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.plugin.opensearch.shared.OpensearchConnection;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class GetValidationTest {
    @Inject
    private ModelValidator modelValidator;

    @Test
    void shouldValidateWithoutDocVersion() {
        assertThat(modelValidator.isValid(task(null)), is(Optional.empty()));
    }

    @Test
    void shouldValidateWithDocVersion() {
        assertThat(modelValidator.isValid(task(Property.ofValue(1L))), is(Optional.empty()));
    }

    private Get task(Property<Long> docVersion) {
        return Get.builder()
            .id("get")
            .type(Get.class.getName())
            .connection(OpensearchConnection.builder().hosts(Property.ofValue(List.of("http://localhost:9200"))).build())
            .index(Property.ofValue("my_index"))
            .key(Property.ofValue("my_id"))
            .docVersion(docVersion)
            .build();
    }
}
