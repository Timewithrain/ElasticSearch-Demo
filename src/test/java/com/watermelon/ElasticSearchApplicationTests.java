package com.watermelon;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
class ElasticSearchApplicationTests {

    @Autowired
    @Qualifier("getRestHighLevelClient")
    private RestHighLevelClient client;

    @Test
    void createIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("test_index");
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    @Test
    void contextLoads() {
    }

}
