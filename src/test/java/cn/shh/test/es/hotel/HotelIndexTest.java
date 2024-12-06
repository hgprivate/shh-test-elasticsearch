package cn.shh.test.es.hotel;

import cn.shh.test.es.pojo.Hotel;
import cn.shh.test.es.service.IHotelService;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

/**
 * 对索引hotel的操作
 */
@Slf4j
@SpringBootTest
public class HotelIndexTest {
    @Autowired
    private IHotelService hotelService;
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    @Test
    public void isActive(){
        log.info("elasticsearchClient: {}", elasticsearchClient);
    }

    /**
     * 创建索引hotel，并插入数据
     */
    @Test
    public void createHotelIndex() throws IOException {
        List<Hotel> hotels = hotelService.list();
        BulkRequest.Builder builder = new BulkRequest.Builder();
        for (Hotel hotel : hotels) {
            builder.operations(op -> op
                    .index(idx -> idx
                            .index("hotel")
                            .id(hotel.getId().toString())
                            .document(hotel)
                    )
            );
        }
        BulkResponse bulkResponse = elasticsearchClient.bulk(builder.build());
        if (bulkResponse.errors()) {
            for (BulkResponseItem item : bulkResponse.items()) {
                if (item.error() != null){
                    log.error(item.error().reason());
                }
            }
        }
    }

    /**
     * 删除索引hotel
     */
    @Test
    public void deleteHotelIndex() throws IOException {
        DeleteRequest.Builder deleteBuilder = new DeleteRequest.Builder();
        deleteBuilder.index("hotel");
        deleteBuilder.id("");
        DeleteResponse deleteResponse = elasticsearchClient.delete(deleteBuilder.build());
        log.info("deleteResponse: {}", deleteResponse.index());

        /*DeleteIndexRequest.Builder builder = new DeleteIndexRequest.Builder();
        builder.index("hotel");
        DeleteResponse deleteResponse = elasticsearchClient.delete(builder.build());
        log.info("deleteResponse:{}", deleteResponse.index());*/
    }

    /**
     * 检查指定索引是否存在
     */
    @Test
    public void existsIndex() throws IOException {
        ExistsRequest.Builder builder = new ExistsRequest.Builder();
        builder.index("hotel");
        builder.id("36934");
        BooleanResponse booleanResponse = elasticsearchClient.exists(builder.build());
        log.info("booleanResponse: {}", booleanResponse.value());
    }
}