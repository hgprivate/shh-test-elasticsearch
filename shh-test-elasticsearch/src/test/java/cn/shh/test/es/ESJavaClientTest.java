package cn.shh.test.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class ESJavaClientTest {
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    @Test
    public void m1(){
        System.out.println(elasticsearchClient);
    }

}
