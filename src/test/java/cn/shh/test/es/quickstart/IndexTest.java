package cn.shh.test.es.quickstart;

import cn.shh.test.es.pojo.Product;
import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * 基于 elasticsearch-java client
 */
@SpringBootTest
public class IndexTest {
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    @Test
    public void m1(){
        System.out.println(elasticsearchClient);
    }

    /**
     * 基于 DSL 来创建并插入一条文档数据
     *
     * @throws IOException
     */
    @Test
    public void test1() throws IOException {
        Product product = new Product("bk-1", "City bike", 123.0);
        IndexResponse response = elasticsearchClient.index(i -> i
                .index("product")
                .id(product.getSku())
                .document(product)
        );
        System.out.println("Indexed with version " + response.version());
    }

    /**
     * 基于 构造器 来创建
     */
    @Test
    public void test2() throws IOException {
        Product product = new Product("bk-2", "City bike2", 123.0);
        IndexRequest.Builder<Product> indexReqBuilder = new IndexRequest.Builder<>();
        indexReqBuilder.index("product");
        indexReqBuilder.id(product.getSku());
        indexReqBuilder.document(product);

        IndexResponse response = elasticsearchClient.index(indexReqBuilder.build());

        System.out.println("Indexed with version " + response.version());
    }
}
