package io.kestra.plugin.opensearch;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.transport.endpoints.BooleanResponse;

import java.io.InputStream;
import java.net.URI;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

final class TestUtils {
    private TestUtils() {}

    static void initData(RunContextFactory runContextFactory, StorageInterface storageInterface, List<String> hosts) throws Exception {
        RunContext runContext = runContextFactory.of();
        OpensearchConnection connection = OpensearchConnection.builder().hosts(Property.of(hosts)).build();

        OpenSearchClient client = new OpenSearchClient(connection.client(runContext));
        BooleanResponse gbif = client.indices().exists(ExistsRequest.of(builder -> builder.index("gbig")));
        if (!gbif.value()) {
            // use a bulk task to init the index
            try (InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("gbif_data.json")) {
                URI uri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), resourceAsStream);

                Bulk put = Bulk.builder()
                    .connection(connection)
                    .from(Property.of(uri.toString()))
                    .chunk(Property.of(10))
                    .build();

                Bulk.Output runOutput = put.run(runContext);

                assertThat(runOutput.getSize(), is(900L));
            }
        }
    }
}
