package io.kestra.plugin.opensearch;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class ScrollTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${openSearch-hosts}")
    private List<String> hosts;

    @Inject
    private StorageInterface storageInterface;

    @BeforeEach
    void initData() throws Exception {
        TestUtils.initData(runContextFactory, storageInterface, hosts);
    }

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        Scroll task = Scroll.builder()
            .connection(OpensearchConnection.builder().hosts(Property.ofValue(hosts)).build())
            .indexes(Property.ofValue(Collections.singletonList("gbif")))
            .request("""
                {
                    "query": {
                        "term": {
                            "key": "925277090"
                        }
                    }
                }""")
            .build();

        Scroll.Output run = task.run(runContext);

        assertThat(run.getSize(), is(1L));
    }

    @Test
    void runFull() throws Exception {
        RunContext runContext = runContextFactory.of();

        Scroll task = Scroll.builder()
            .connection(OpensearchConnection.builder().hosts(Property.ofValue(hosts)).build())
            .indexes(Property.ofValue(Collections.singletonList("gbif")))
            .request("""
                {
                    "query": {
                        "match_all": {}
                    }
                }""")
            .build();

        Scroll.Output run = task.run(runContext);

        assertThat(run.getSize(), is(900L));
    }
}
