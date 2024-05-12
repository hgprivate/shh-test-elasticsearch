package cn.shh.test.es.hotel;

import cn.shh.test.es.pojo.Hotel;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldSort;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOptionsBuilders;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.*;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.util.ObjectBuilder;
import co.elastic.clients.util.ObjectBuilderBase;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 作者：shh
 * 时间：2023/6/27
 * 版本：v1.0
 */
@Slf4j
@SpringBootTest
public class HotelSearchTest {
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    @Test
    public void isActive() {
        System.out.println("elasticsearchClient = " + elasticsearchClient);
    }

    /**
     * 匹配所有的文档数据
     */
    @Test
    public void matchAll() throws IOException {
        SearchResponse<Hotel> searchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(
                                QueryBuilders.matchAll()
                                        .build()._toQuery()
                        )
                        .build(),
                Hotel.class
        );
        handlerSearchResponse(searchResponse);
    }

    /**
     * 匹配 某个字段值符合要求的数据
     */
    @Test
    public void matchQuery() throws IOException {
        SearchResponse<Hotel> searchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(
                                QueryBuilders.match()
                                        .field("name").query("如家")
                                        .build()._toQuery()
                        )
                        .build(),
                Hotel.class
        );
        handlerSearchResponse(searchResponse);
    }

    /**
     * 组合查询
     */
    @Test
    public void testBool() throws IOException {
        SearchResponse<Hotel> boolSearchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(
                                QueryBuilders.bool()
                                        .must(
                                                QueryBuilders.term()
                                                        .field("city").value("上海")
                                                        .build()._toQuery()
                                        )
                                        .filter(
                                                QueryBuilders.range()
                                                        .field("price").lte(JsonData.of(200))
                                                        .build()._toQuery()
                                        )
                                        .build()._toQuery()
                        )
                        .build(),
                Hotel.class
        );
        handlerSearchResponse(boolSearchResponse);
    }

    /**
     * 升序/降序展示，且限定展示数据的条数
     */
    @Test
    public void testPageAndSort() throws IOException {
        SearchResponse<Hotel> searchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(
                                QueryBuilders.matchAll().build()._toQuery()
                        )
                        .sort(
                                SortOptionsBuilders.field(builder -> builder.field("id").order(SortOrder.Asc)),
                                SortOptionsBuilders.field(builder -> builder.field("price").order(SortOrder.Asc))
                        )
                        .from(0)
                        .size(5)
                        .build(),
                Hotel.class
        );
        handlerSearchResponse(searchResponse);
    }

    /**
     * 测试高亮效果
     */
    @Test
    public void testHightlight() throws IOException {
        SearchResponse<Hotel> searchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(
                                QueryBuilders.match().field("name").query("假日").build()._toQuery()
                        )
                        .highlight(
                                hightlightBuilder -> hightlightBuilder
                                        .fields(
                                                "name",
                                                hightlightFieldBuilder -> hightlightFieldBuilder
                                                        .matchedFields("name", "address")
                                                        .requireFieldMatch(false)
                                        )
                        )
                        .build(),
                Hotel.class
        );
        log.info("searchResponse: {}", searchResponse);
        List<Hit<Hotel>> hitList = searchResponse.hits().hits();
        hitList.get(1).highlight().get("name").forEach(System.out::println);
    }

    private void handlerSearchResponse(SearchResponse<Hotel> boolSearchResponse) {
        HitsMetadata<Hotel> hitsMetadata = boolSearchResponse.hits();
        log.info("数据条数：{}", hitsMetadata.total().value());
        List<Hit<Hotel>> hitList = hitsMetadata.hits();
        for (Hit<Hotel> hotelHit : hitList) {
            log.info("hotel: {}", hotelHit.source());
        }
    }

    /**
     * 多个字段匹配
     *
     * @throws IOException
     */
    @Test
    public void testMatchQuery() throws IOException {
        SearchResponse<Hotel> searchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(
                                QueryBuilders.multiMatch()
                                        .fields("name", "brand").query("速8")
                                        .build()._toQuery()
                        )
                        .build(),
                Hotel.class
        );
        List<Hit<Hotel>> hitList = searchResponse.hits().hits();
        for (Hit<Hotel> hotelHit : hitList) {
            Hotel hotel = hotelHit.source();
            log.info("hotel: {}", hotel);
        }
    }

    @Test
    public void testAggregation() throws IOException {
        SearchResponse<Hotel> searchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(
                                QueryBuilders.matchAll().build()._toQuery()
                        )
                        .size(0)
                        .aggregations(
                                "brandAgg",
                                aggBuilder -> aggBuilder.terms(
                                        termAggBuilder -> termAggBuilder.field("brand.keyword")
                                ).aggregations(
                                        "priceStats",
                                        aggBuilder2 -> aggBuilder2.stats(
                                                statsAggBuilder -> statsAggBuilder.field("price")
                                        )
                                )
                        )
                        .build(),
                Hotel.class
        );
        List<StringTermsBucket> stringTermsBuckets = searchResponse.aggregations().get("brandAgg").sterms().buckets().array();
        for (StringTermsBucket stringTermsBucket : stringTermsBuckets) {
            StatsAggregate priceStats = stringTermsBucket.aggregations().get("priceStats").stats();
            log.info("key: {}, doc_count: {}, priceStats: {count: {}, min: {}, max: {}, avg: {}, sum: {}}",
                    stringTermsBucket.key().stringValue(),
                    stringTermsBucket.docCount(),
                    priceStats.count(),
                    priceStats.min(),
                    priceStats.max(),
                    priceStats.avg(),
                    priceStats.sum()
            );
        }
    }
}