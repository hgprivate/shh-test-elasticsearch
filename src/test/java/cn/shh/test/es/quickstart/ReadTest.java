package cn.shh.test.es.quickstart;

import cn.shh.test.es.pojo.Product;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetResponse;
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
public class ReadTest {
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    /**
     * 根据ID来读取单个document
     */
    @Test
    public void getById() throws IOException {
        GetResponse<Product> response = elasticsearchClient.get(g ->
                        g.index("product").id("bk-1"), Product.class);

        if (response.found()) {
            Product product = response.source();
            log.info("Product name：" + product.getName());
        } else {
            log.info ("Product not found");
        }
    }

    @Test
    public void getByIdJson() throws IOException {
        GetResponse<ObjectNode> response = elasticsearchClient.get(g ->
                        g.index("product").id("bk-1"), ObjectNode.class);

        if (response.found()) {
            ObjectNode json = response.source();
            String name = json.get("name").asText();
            log.info("Product name: " + name);
        } else {
            log.info("Product not found");
        }
    }
}