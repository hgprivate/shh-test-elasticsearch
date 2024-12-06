package cn.shh.test.es.quickstart;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.HistogramBucket;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.JsonData;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * client elasticsearch-java 聚合操作
 */
@Slf4j
@SpringBootTest
public class AggregationsTest {
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    private static final SearchResponse<JsonData> searchResponse = SearchResponse.of(b -> b
            .aggregations(new HashMap<>())
            .took(0)
            .timedOut(false)
            .hits(h -> h
                    .total(t -> t.value(0).relation(TotalHitsRelation.Eq))
                    .hits(new ArrayList<>())
            )
            .shards(s -> s
                    .total(1)
                    .failed(0)
                    .successful(1)
            )
            .aggregations("price-histogram", a -> a.histogram(h -> h
                    .buckets(bu -> bu.array(Collections.singletonList(HistogramBucket.of(hb -> hb
                            .key(50).docCount(1)
                    ))))
            ))
    );

    @Test
    public void priceHistogram() throws Exception {
        //tag::price-histo-request
        Query query = MatchQuery.of(m -> m.field("name").query("bike"))._toQuery();
        SearchResponse<Void> response = elasticsearchClient.search(b -> b
                        .index("product")
                        .size(0) // <1>
                        .query(query) // <2>
                        .aggregations("price-histogram", a -> a // <3>
                                .histogram(h -> h // <4>
                                        .field("price")
                                        .interval(50.0)
                                )
                        ), Void.class // <5>
        );

        List<HistogramBucket> buckets = response.aggregations()
                .get("price-histogram") // <1>
                .histogram() // <2>
                .buckets().array(); // <3>

        for (HistogramBucket bucket: buckets) {
            log.info("There are " + bucket.docCount() + " bikes under " + bucket.key());
        }
    }
}