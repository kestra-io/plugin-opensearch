package io.kestra.plugin.opensearch;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.opensearch.shared.OpensearchConnection;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@KestraTest
class SearchTotalRegressionTest {
    @Inject
    private RunContextFactory runContextFactory;

    private HttpServer server;

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            // the response OpenSearch sends for `track_total_hits: false`: no `hits.total`
            byte[] payload = """
                {"took":1,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},
                "hits":{"max_score":1.0,"hits":[{"_index":"test","_id":"1","_score":1.0,"_source":{"name":"one"}}]}}"""
                .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, payload.length);
            try (var output = exchange.getResponseBody()) {
                output.write(payload);
            }
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void searchWithoutTrackedTotalHits() throws Exception {
        Search task = Search.builder()
            .id(UUID.randomUUID().toString())
            .type(Search.class.getName())
            .connection(OpensearchConnection.builder().hosts(Property.ofValue(List.of("http://127.0.0.1:" + server.getAddress().getPort()))).build())
            .indexes(Property.ofValue(List.of("test")))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .request("{\"track_total_hits\":false,\"query\":{\"match_all\":{}}}")
            .build();

        Search.Output output = task.run(runContextFactory.of());

        assertEquals(1, output.getSize());
        assertEquals("one", output.getRows().getFirst().get("name"));
        assertNull(output.getTotal());
    }
}
