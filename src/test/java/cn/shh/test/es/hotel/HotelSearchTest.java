package cn.shh.test.es.hotel;

import cn.shh.test.es.pojo.Hotel;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOptions;
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
        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index("hotel");
        builder.query(QueryBuilders.matchAll().build()._toQuery());
        SearchResponse<Hotel> searchResponse = elasticsearchClient.search(builder.build(), Hotel.class);
        handlerSearchResponse(searchResponse);
    }

    /**
     * 匹配 某个字段值符合要求的数据
     */
    @Test
    public void matchQuery() throws IOException {
        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index("hotel");
        builder.query(QueryBuilders.match()
                .field("name")
                .query("如家")
                .build()
                ._toQuery()
        );
        SearchResponse<Hotel> searchResponse = elasticsearchClient.search(builder.build(), Hotel.class);
        handlerSearchResponse(searchResponse);
    }

    /**
     * 组合查询
     */
    @Test
    public void testBool() throws IOException {
        BoolQuery boolQuery = BoolQuery.of(b -> b.must(TermQuery.of(t -> t.field("city").value("上海"))._toQuery()));
        BoolQuery boolQuery2 = BoolQuery.of(b -> b.filter(RangeQuery.of(r -> r.field("price").lte(JsonData.of(200)))._toQuery()));

        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index("hotel");
        builder.query(Query.of(q -> q.bool(boolQuery)));
        builder.query(Query.of(q -> q.bool(boolQuery2)));

        SearchResponse<Hotel> boolSearchResponse = elasticsearchClient.search(builder.build(), Hotel.class);
        handlerSearchResponse(boolSearchResponse);
    }

    /**
     * 升序/降序展示，且限定展示数据的条数
     */
    @Test
    public void testPageAndSort() throws IOException {
        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index("hotel");
        builder.query(QueryBuilders.matchAll().build()._toQuery());
        builder.sort(s -> s.field(f -> f.field("price").order(SortOrder.Asc)));
        builder.from(0).size(5);

        SearchResponse<Hotel> searchResponse = elasticsearchClient.search(builder.build(), Hotel.class);
        handlerSearchResponse(searchResponse);
    }

    /**
     * 测试高亮效果
     */
    @Test
    public void testHightlight() throws IOException {
        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index("hotel");
        builder.query(q -> q.match(m -> m.field("name").query("假日")));
        builder.highlight(h -> h.fields("name", hf -> hf.matchedFields("name", "address").requireFieldMatch(false)));

        SearchResponse<Hotel> searchResponse = elasticsearchClient.search(builder.build(), Hotel.class);
        handlerSearchResponse(searchResponse);
        //log.info("searchResponse: {}", searchResponse);
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
        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index("hotel");
        //builder.query(MatchQuery.of(b -> b.field("city").query("上海"))._toQuery());
        builder.query(MultiMatchQuery.of(b -> b.fields("name", "brand").query("速8"))._toQuery());

        SearchResponse<Hotel> searchResponse = elasticsearchClient.search(builder.build(), Hotel.class);
        List<Hit<Hotel>> hitList = searchResponse.hits().hits();
        for (Hit<Hotel> hotelHit : hitList) {
            Hotel hotel = hotelHit.source();
            log.info("hotel: {}", hotel);
        }
    }

    @Test
    public void testAggregation() throws IOException {
        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index("hotel");
        builder.query(q -> q.match(mq -> mq.field("brand").query("速8")));
        builder.aggregations("brandAgg", a -> a.histogram(ha -> ha.field("price").interval(50.0)));

        SearchResponse<Hotel> searchResponse = elasticsearchClient.search(builder.build(), Hotel.class);
        List<HistogramBucket> histogramBuckets = searchResponse.aggregations().get("brandAgg").histogram().buckets().array();
        for (HistogramBucket histogramBucket : histogramBuckets) {
            log.info("doc count: {}, key: {}", histogramBucket.docCount(), histogramBucket.key());
        }
    }
}