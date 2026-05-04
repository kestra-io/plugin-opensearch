package io.kestra.plugin.opensearch;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@KestraTest
class PplTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Value("${openSearch-hosts}")
    private List<String> hosts;

    @BeforeEach
    void initData() throws Exception {
        TestUtils.initData(runContextFactory, storageInterface, hosts);
    }

    @Test
    void runJdbcFetch() throws Exception {
        RunContext runContext = runContextFactory.of();

        Ppl task = Ppl.builder()
            .connection(OpensearchConnection.builder().hosts(Property.ofValue(hosts)).build())
            .query(Property.ofValue("source=gbif | where publishingCountry='BE' | stats count() by genus"))
            .fetchSize(Property.ofValue(50))
            .format(Property.ofValue(Ppl.Format.JDBC))
            .build();

        Ppl.Output output = task.run(runContext);

        assertThat(output.getRows(), notNullValue());
        assertThat(output.getRows().size(), greaterThan(0));
        assertThat(output.getRows().getFirst(), hasKey("genus"));
        assertThat(output.getRows().getFirst(), hasKey("count()"));
        assertThat(output.getSize(), is(output.getRows().size()));
        assertThat(output.getTotal(), notNullValue());
        assertThat(output.getRow(), nullValue());
        assertThat(output.getUri(), nullValue());
        assertThat(output.getText(), nullValue());
    }

    @Test
    void runJdbcFetchOne() throws Exception {
        RunContext runContext = runContextFactory.of();

        Ppl task = Ppl.builder()
            .connection(OpensearchConnection.builder().hosts(Property.ofValue(hosts)).build())
            .query(Property.ofValue("source=gbif | where publishingCountry='BE' | stats count() by genus"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        Ppl.Output output = task.run(runContext);

        assertThat(output.getRow(), notNullValue());
        assertThat(output.getRow(), hasKey("genus"));
        assertThat(output.getSize(), is(1));
        assertThat(output.getRows(), nullValue());
    }

    @Test
    void runJdbcStore() throws Exception {
        RunContext runContext = runContextFactory.of();

        Ppl task = Ppl.builder()
            .connection(OpensearchConnection.builder().hosts(Property.ofValue(hosts)).build())
            .query(Property.ofValue("source=gbif | where publishingCountry='BE' | stats count() by genus"))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();

        Ppl.Output output = task.run(runContext);

        assertThat(output.getUri(), notNullValue());
        assertThat(output.getSize(), greaterThan(0));
        assertThat(output.getRows(), nullValue());

        long lines;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
            storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())))) {
            lines = reader.lines().count();
        }
        assertThat((int) lines, is(output.getSize()));
    }

    @Test
    void runCsv() throws Exception {
        RunContext runContext = runContextFactory.of();

        Ppl task = Ppl.builder()
            .connection(OpensearchConnection.builder().hosts(Property.ofValue(hosts)).build())
            .query(Property.ofValue("source=gbif | where publishingCountry='BE' | stats count() by genus"))
            .format(Property.ofValue(Ppl.Format.CSV))
            .build();

        Ppl.Output output = task.run(runContext);

        assertThat(output.getText(), notNullValue());
        assertThat(output.getText(), containsString("count()"));
        assertThat(output.getRows(), nullValue());
        assertThat(output.getUri(), nullValue());
    }

    @Test
    void runRaw() throws Exception {
        RunContext runContext = runContextFactory.of();

        Ppl task = Ppl.builder()
            .connection(OpensearchConnection.builder().hosts(Property.ofValue(hosts)).build())
            .query(Property.ofValue("source=gbif | where publishingCountry='BE' | stats count() by genus"))
            .format(Property.ofValue(Ppl.Format.RAW))
            .build();

        Ppl.Output output = task.run(runContext);

        assertThat(output.getText(), notNullValue());
        assertThat(output.getText(), containsString("count()"));
        assertThat(output.getText(), containsString("|"));
        assertThat(output.getRows(), nullValue());
        assertThat(output.getUri(), nullValue());
    }
}
