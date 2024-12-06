package cn.shh.test.es.quickstart;

import cn.shh.test.es.pojo.Product;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.SearchTemplateResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.JsonData;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

/**
 * client elasticsearch-java 搜索操作
 */
@Slf4j
@SpringBootTest
public class SearchTest {
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    /**
     * 简单搜索
     */
    @Test
    public void simpleSearch() throws IOException {
        SearchResponse<Product> response = elasticsearchClient.search(s ->
                s.index("product").query(q ->
                        q.match(t -> t.field("name").query("bike"))
                ), Product.class);

        TotalHits total = response.hits().total();
        boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

        if (isExactResult) {
            log.info("There are " + total.value() + " results");
        } else {
            log.info("There are more than " + total.value() + " results");
        }

        List<Hit<Product>> hits = response.hits().hits();
        for (Hit<Product> hit: hits) {
            Product product = hit.source();
            log.info("Found product " + product.getSku() + ", score " + hit.score());
        }
    }

    /**
     * 嵌套搜索
     */
    @Test
    public void searchNested() throws Exception {
        //transport.setResult(searchResponse);
        //tag::search-nested

        // Search by product name
        Query byName = MatchQuery.of(m -> m.field("name").query("bike"))._toQuery();

        // Search by max price
        Query byMaxPrice = RangeQuery.of(r -> r.field("price").gte(JsonData.of(200.0)))._toQuery();

        // Combine name and price queries to search the product index
        SearchResponse<Product> response = elasticsearchClient.search(s -> s
                        .index("product")
                        .query(q -> q.bool(b -> b.must(byName).must(byMaxPrice))), Product.class);

        List<Hit<Product>> hits = response.hits().hits();
        for (Hit<Product> hit: hits) {
            Product product = hit.source();
            log.info("Found product " + product.getSku() + ", score " + hit.score());
        }
        //end::search-nested
    }

    /**
     * 模板化搜索
     */
    @Test
    public void searchTemplate() throws Exception {
        elasticsearchClient.putScript(r -> r
                .id("query-script") // <1>
                .script(s -> s
                        .lang("mustache")
                        .source("{\"query\":{\"match\":{\"{{field}}\":\"{{value}}\"}}}")
                ));

        SearchTemplateResponse<Product> response = elasticsearchClient.searchTemplate(r -> r
                        .index("product")
                        .id("query-script") // <1>
                        .params("field", JsonData.of("name")) // <2>
                        .params("value", JsonData.of("bike")), Product.class);

        List<Hit<Product>> hits = response.hits().hits();
        for (Hit<Product> hit: hits) {
            Product product = hit.source();
            log.info("Found product " + product.getSku() + ", score " + hit.score());
        }
    }
}