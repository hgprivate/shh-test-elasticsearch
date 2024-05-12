package cn.shh.test.es.quickstart;

import cn.shh.test.es.pojo.Product;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.CreateRequest;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * 作者：shh
 * 时间：2023/6/26
 * 版本：v1.0
 *
 * 读取测试
 */
@Slf4j
@SpringBootTest
public class DocTest {
    private final String INDEX_NAME = "product";
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    /**
     * 添加一个文档
     *
     * @throws IOException
     */
    @Test
    public void testAddDoc() throws IOException {
        Product product = new Product();
        product.setName("摩托车");
        product.setPrice(10000);
        product.setSku("sku2");
        elasticsearchClient.create(CreateRequest.of(builder -> builder.index(INDEX_NAME).id("2").document(product)));
    }

    /**
     * 根据ID来读取单个document
     */
    @Test
    public void getById() throws IOException {
        GetResponse<Product> response = elasticsearchClient.get(g ->
                        g.index("product").id("1"), Product.class);

        if (response.found()) {
            Product product = response.source();
            log.info("Product name：" + product.getName());
        } else {
            log.info ("Product not found");
        }
    }
    /*@Test
    public void getByIdJson() throws IOException {
        GetResponse<ObjectNode> response = elasticsearchClient.get(g ->
                        g.index("product").id("2"), ObjectNode.class);
        if (response.found()) {
            ObjectNode json = response.source();
            String name = json.get("name").asText();
            log.info("Product name: " + name);
        } else {
            log.info("Product not found");
        }
    }*/

    /**
     * 修改一个文档数据
     *
     * @throws IOException
     */
    @Test
    public void testUpdateDoc() throws IOException {
        Product product = new Product();
        product.setName("摩托车");
        product.setPrice(16000);
        product.setSku("sku2");
        elasticsearchClient.update(UpdateRequest.of(builder ->
                        builder.index(INDEX_NAME).id("2").doc(product)),
                Product.class);
    }

    /**
     * 删除一个文档
     * @throws IOException
     */
    @Test
    public void testDeleteDoc() throws IOException {
        //elasticsearchClient.delete(new DeleteRequest.Builder().index(INDEX_NAME).id("2").build());
        elasticsearchClient.delete(DeleteRequest.of(builder -> builder.index(INDEX_NAME).id("2")));
    }
}