package cn.shh.test.es.hotel;

import cn.shh.test.es.pojo.HotelDoc;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.*;
import co.elastic.clients.json.JsonData;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * 针对索引hotel下文档的一系列查询操作
 */
@Slf4j
@SpringBootTest
public class HotelDocSearchTest {
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    @Test
    public void isActive() {
        System.out.println("elasticsearchClient = " + elasticsearchClient);
    }

    /**
     * 查询所有文档数据
     */
    @Test
    public void matchAll() throws IOException {
        SearchResponse<HotelDoc> searchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(
                                QueryBuilders.matchAll().build()._toQuery()
                        ).build(), HotelDoc.class);
        handlerSearchResponse(searchResponse);
    }

    /**
     * 查询符合单个字段值条件的文档数据
     */
    @Test
    public void matchQuery() throws IOException {
        SearchResponse<HotelDoc> searchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(
                                QueryBuilders.match()
                                .field("name").query("如家")
                                .build()._toQuery()
                        ).build(), HotelDoc.class);
        handlerSearchResponse(searchResponse);
    }

    /**
     * 查询符合多个字段值条件的文档数据
     *
     * @throws IOException
     */
    @Test
    public void testMatchQuery() throws IOException {
        SearchResponse<HotelDoc> searchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(
                                QueryBuilders.multiMatch()
                                .fields("name", "brand")
                                .query("速8")
                                .build()._toQuery()
                        ).build(), HotelDoc.class);
        List<Hit<HotelDoc>> hitList = searchResponse.hits().hits();
        for (Hit<HotelDoc> hotelHit : hitList) {
            HotelDoc hotel = hotelHit.source();
            log.info("hotel: {}", hotel);
        }
    }

    /**
     * 查询符合经纬度条件的文档数据 31.21, 121.5
     *
     * @throws IOException
     */
    @Test
    public void testGeoDistanceQuery() throws IOException {
        SearchResponse<HotelDoc> searchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(
                                QueryBuilders.geoDistance()
                                .field("location")
                                .distance("2km")
                                .location(geoLocationBuilder -> geoLocationBuilder.text("31.21, 121.5"))
                                .build()._toQuery()
                        ).build(), HotelDoc.class);
        List<Hit<HotelDoc>> hitList = searchResponse.hits().hits();
        for (Hit<HotelDoc> hotelHit : hitList) {
            HotelDoc hotel = hotelHit.source();
            log.info("hotel: {}", hotel);
        }
    }

    /**
     * 组合多个条件来查询
     */
    @Test
    public void testBool() throws IOException {
        SearchResponse<HotelDoc> boolSearchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(
                                QueryBuilders.bool()
                                .must(
                                        QueryBuilders.term()
                                        .field("city").value("上海")
                                        .build()._toQuery()
                                ).filter(
                                        QueryBuilders.range()
                                        .field("price").lte(JsonData.of(200))
                                        .build()._toQuery()
                                ).build()._toQuery()
                        ).build(), HotelDoc.class);
        handlerSearchResponse(boolSearchResponse);
    }

    /**
     * 查询所有文档数据，并对查询结果分页和排序
     */
    @Test
    public void testPageAndSort() throws IOException {
        SearchResponse<HotelDoc> searchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(QueryBuilders.matchAll().build()._toQuery())
                        .sort(
                                SortOptionsBuilders.field(builder -> builder.field("id").order(SortOrder.Asc)),
                                SortOptionsBuilders.field(builder -> builder.field("price").order(SortOrder.Asc))
                        ).from(0)
                        .size(5)
                        .build(),
                HotelDoc.class);
        handlerSearchResponse(searchResponse);
    }
    private void handlerSearchResponse(SearchResponse<HotelDoc> boolSearchResponse) {
        HitsMetadata<HotelDoc> hitsMetadata = boolSearchResponse.hits();
        log.info("数据条数：{}", hitsMetadata.total().value());
        List<Hit<HotelDoc>> hitList = hitsMetadata.hits();
        for (Hit<HotelDoc> hotelHit : hitList) {
            log.info("hotel: {}", hotelHit.source());
        }
    }

    /**
     * 查询结果高亮展示
     */
    @Test
    public void testHightlight() throws IOException {
        SearchResponse<HotelDoc> searchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(QueryBuilders.match().field("name").query("假日").build()._toQuery())
                        .highlight(hightlightBuilder -> hightlightBuilder
                                        .fields("name", hightlightFieldBuilder -> hightlightFieldBuilder
                                                        .matchedFields("name", "address")
                                                        .requireFieldMatch(false))
                        ).build(), HotelDoc.class);
        log.info("searchResponse: {}", searchResponse);
        List<Hit<HotelDoc>> hitList = searchResponse.hits().hits();
        hitList.get(1).highlight().get("name").forEach(System.out::println);
    }

    /**
     * 查询结果聚合统计
     *
     * @throws IOException
     */
    @Test
    public void testAggregation() throws IOException {
        SearchResponse<HotelDoc> searchResponse = elasticsearchClient.search(
                new SearchRequest.Builder().index("hotel")
                        .query(QueryBuilders.matchAll().build()._toQuery())
                        .size(0)
                        .aggregations("brandAgg", aggBuilder -> aggBuilder.terms(
                                        termAggBuilder -> termAggBuilder.field("brand")
                                ).aggregations("priceStats", aggBuilder2 -> aggBuilder2.stats(
                                                statsAggBuilder -> statsAggBuilder.field("price"))
                                )
                        ).build(), HotelDoc.class);
        List<StringTermsBucket> stringTermsBuckets = searchResponse.aggregations()
                .get("brandAgg").sterms().buckets().array();
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