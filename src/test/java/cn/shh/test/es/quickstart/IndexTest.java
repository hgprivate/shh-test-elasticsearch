package cn.shh.test.es.quickstart;

import cn.shh.test.es.pojo.Product;
import cn.shh.test.es.util.ElasticSearchUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * elasticsearch-java client之索引操作
 */
@SpringBootTest
public class IndexTest {
    private final String INDEX_NAME = "product";
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    /**
     * 创建索引并添加一条文档数据
     */
    @Test
    public void testCreateIndexAndAddDoc() throws IOException {
        //ElasticsearchClient elasticsearchClient = ElasticSearchUtil.elasticsearchClient();
        Product product = new Product("bk-1", "City bike", 123.0);
        IndexResponse response = elasticsearchClient.index(i -> i
                .index(INDEX_NAME)
                .id(product.getSku())
                .document(product)
        );
        System.out.println("Indexed with version " + response.version());
    }

    /**
     * 基于构建器来创建索引并添加一条文档数据
     */
    @Test
    public void testCreateIndexAndAddDoc2() throws IOException {
        //ElasticsearchClient elasticsearchClient = ElasticSearchUtil.elasticsearchClient();
        Product product = new Product("bk-2", "City bike2", 123.0);
        IndexRequest.Builder<Product> indexReqBuilder = new IndexRequest.Builder<>();
        indexReqBuilder.index(INDEX_NAME);
        indexReqBuilder.id(product.getSku());
        indexReqBuilder.document(product);

        IndexResponse response = elasticsearchClient.index(indexReqBuilder.build());

        System.out.println("Indexed with version " + response.version());
    }


    /**
     * 删除一个索引
     * @throws IOException
     */
    @Test
    public void testDeleteIndex() throws IOException {
        elasticsearchClient.indices().delete(new DeleteIndexRequest.Builder().index(INDEX_NAME).build());
    }

    /**
     * 批量操作多个索引
     */
    @Test
    public void testCreateIndexAndAddDocBulk() throws IOException {
        List<Product> products = Stream.of(
                new Product("bk-3", "City bike3", 123.0),
                new Product("bk-4", "City bike4", 100.0),
                new Product("bk-5", "City bike5", 188.0)
        ).collect(Collectors.toList());

        BulkRequest.Builder br = new BulkRequest.Builder();
        for (Product product : products) {
            br.operations(op -> op.index(idx -> idx.index("product")
                            .id(product.getSku()).document(product)));
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