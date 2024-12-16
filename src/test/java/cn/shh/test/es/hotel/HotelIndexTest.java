package cn.shh.test.es.hotel;

import cn.shh.test.es.pojo.Hotel;
import cn.shh.test.es.pojo.HotelDoc;
import cn.shh.test.es.service.IHotelService;
import cn.shh.test.es.util.HotelConstant;
import co.elastic.clients.elasticsearch.ElasticsearchClient;

import co.elastic.clients.elasticsearch._types.mapping.KeywordProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.PropertyBuilders;
import co.elastic.clients.elasticsearch._types.mapping.SourceField;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 针对索引hotel的一系列操作
 */
@Slf4j
@SpringBootTest
public class HotelIndexTest {
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    /**
     * 检查指定索引是否存在
     */
    @Test
    public void existsIndex() throws IOException {
        ExistsRequest.Builder builder = new ExistsRequest.Builder();
        builder.index("hotel");
        BooleanResponse exists = elasticsearchClient.indices().exists(builder.build());
        log.info("BooleanResponse: {}", exists.value());
    }

    /**
     * 创建索引hotel，并设置映射
     */
    @Test
    public void createHotelDocIndex() throws IOException {
        // 1、创建索引 hotel
        CreateIndexResponse createIndexResponse = elasticsearchClient.indices().create(createIndexRequestBuilder ->
                createIndexRequestBuilder.index("hotel"));
        log.info("CreateIndexResponse: {}", createIndexResponse.index());
        // 2、为索引 hotel 设置 mapping
        PutMappingResponse putMappingResponse = elasticsearchClient.indices().putMapping(putMappingRequestBuilder -> {
            Map<String, Property> map = new HashMap<>();
            map.put("id", PropertyBuilders.keyword().build()._toProperty());
            map.put("name", PropertyBuilders.text().analyzer("ik_max_word").copyTo("all").build()._toProperty());
            map.put("address", PropertyBuilders.keyword().index(false).build()._toProperty());
            map.put("price", PropertyBuilders.integer().build()._toProperty());
            map.put("score", PropertyBuilders.integer().build()._toProperty());
            map.put("brand", PropertyBuilders.keyword().copyTo("all").build()._toProperty());
            map.put("city", PropertyBuilders.keyword().copyTo("all").build()._toProperty());
            map.put("starName", PropertyBuilders.keyword().build()._toProperty());
            map.put("business", PropertyBuilders.keyword().build()._toProperty());
            map.put("location", PropertyBuilders.geoPoint().build()._toProperty());
            map.put("pic", PropertyBuilders.keyword().index(false).build()._toProperty());
            map.put("all", PropertyBuilders.text().analyzer("ik_max_word").build()._toProperty());
            return putMappingRequestBuilder.index("hotel").properties(map);
        });
        log.info("PutMappingResponse: {}", putMappingResponse.toString());
    }

    /**
     * 删除索引hotel
     */
    @Test
    public void deleteHotelDocIndex() throws IOException {
        DeleteIndexRequest.Builder builder = new DeleteIndexRequest.Builder();
        builder.index("hotel");
        DeleteIndexResponse deleteIndexResponse = elasticsearchClient.indices().delete(builder.build());
        log.info("deleteResponse: {}", deleteIndexResponse);
    }
}