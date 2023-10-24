package cn.shh.test.es.quickstart;

import cn.shh.test.es.pojo.Product;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 基于 elasticsearch-java client
 */
@SpringBootTest
public class IndexBulkTest {
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    @Test
    public void m1(){
        System.out.println(elasticsearchClient);
    }

    /**
     * 批量操作多个索引
     */
    @Test
    public void test() throws IOException {
       List<Product> products = Stream.of(
               new Product("bk-3", "City bike3", 123.0),
               new Product("bk-4", "City bike4", 100.0),
               new Product("bk-5", "City bike5", 188.0)
       ).collect(Collectors.toList());

       BulkRequest.Builder br = new BulkRequest.Builder();
       for (Product product : products) {
           br.operations(op -> op.index(idx ->
                   idx.index("product").id(product.getSku()).document(product)));
       }
       BulkResponse result = elasticsearchClient.bulk(br.build());

       if (result.errors()) {
           for (BulkResponseItem item: result.items()) {
               if (item.error() != null) {
                   System.err.println(item.error().reason());
               }
           }
       }
   }
}