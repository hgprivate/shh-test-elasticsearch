package cn.shh.test.es.hotel;

import cn.shh.test.es.pojo.Hotel;
import cn.shh.test.es.service.IHotelService;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryVariant;
import co.elastic.clients.elasticsearch.core.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * 索引hotel下的文档查询操作
 */
@Slf4j
@SpringBootTest
public class HotelDocumentTest {
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    @Test
    public void isActive() {
        log.info("elasticsearchClient: {}", elasticsearchClient);
    }

    /**
     * 给 hotel index 添加一条文档数据
     */
    @Test
    public void addDocument() throws IOException {
        // 2062643512	深圳国际会展中心希尔顿酒店	展丰路80号	285	46	希尔顿	深圳	五钻	深圳国际会展中心商圈	22.705335	113.77794	https://m.tuniucdn.com/fb3/s1/2n9c/2SHUVXNrN5NsXsTUwcd1yaHKbrGq_w200_h200_c1_t0.jpg
        Hotel hotel = new Hotel();
        hotel.setId(12345678L);
        hotel.setName("测试酒店");
        hotel.setAddress("北京");
        IndexResponse indexResponse = elasticsearchClient.index(i -> i
                .index("hotel")
                .id(hotel.getId().toString())
                .document(hotel));
        log.info("indexResponse: {}", indexResponse.index());
    }

    /**
     * 根据id获取文档
     */
    @Test
    public void getById() throws IOException {
        GetResponse<Hotel> getResponse = elasticsearchClient.get(g -> g
                .index("hotel").id("12345678"), Hotel.class);
        if (getResponse.found()){
            Hotel hotel = getResponse.source();
            log.info("hotel: {}", hotel);
        }else {
            log.info("hotel not found.");
        }
    }

    /**
     * 修改文档
     */
    @Test
    public void updateDocument() throws IOException {
        Hotel hotel = elasticsearchClient.get(g -> g
                .index("hotel").id("12345678"), Hotel.class).source();
        hotel.setName("测试酒店666");
        UpdateRequest.Builder builder = new UpdateRequest.Builder<Hotel, Hotel>();
        builder.index("hotel");
        builder.id("12345678");
        builder.doc(hotel);
        UpdateResponse<Hotel> updateResponse = elasticsearchClient.update(builder.build(), Hotel.class);
        log.info("updateResponse: {}", updateResponse.index());
    }

    /**
     * 根据id删除文档
     */
    @Test
    public void deleteById() throws IOException {
        DeleteRequest.Builder builder = new DeleteRequest.Builder();
        builder.index("hotel");
        builder.id("12345678");
        DeleteResponse deleteResponse = elasticsearchClient.delete(builder.build());
        log.info("deleteResponse: {}", deleteResponse.index());
    }

    /**
     * 根据条件删除文档
     */
    @Test
    public void deleteAll() throws IOException {
        DeleteByQueryRequest.Builder queryRequestBuilder = new DeleteByQueryRequest.Builder();
        queryRequestBuilder.index("hotel");
        queryRequestBuilder.query(QueryBuilders.matchAll().build()._toQuery());
        DeleteByQueryResponse deleteByQueryResponse = elasticsearchClient.deleteByQuery(queryRequestBuilder.build());
        log.info("deleteByQueryResponse: {}", deleteByQueryResponse.batches());
    }
}