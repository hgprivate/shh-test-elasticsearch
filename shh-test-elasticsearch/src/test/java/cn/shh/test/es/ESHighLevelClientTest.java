package cn.shh.test.es;

import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class ESHighLevelClientTest {
    @Autowired
    private RestHighLevelClient rhlClient;

    @Test
    public void m1(){
        System.out.println(rhlClient);
    }

}
