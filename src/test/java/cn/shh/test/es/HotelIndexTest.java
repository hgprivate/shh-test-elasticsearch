package cn.shh.test.es;

import cn.shh.test.es.util.HotelConstant;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
public class HotelIndexTest {
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    public void m1(){
        System.out.println(restHighLevelClient);
    }

    /**
     * 创建酒店index（表结构）
     *
     * @throws IOException
     */
    @Test
    void createHotelIndex() throws IOException {
        //指定索引库名
        CreateIndexRequest hotel = new CreateIndexRequest("hotel");
        //写入JSON数据，这里是Mapping映射
        hotel.source(HotelConstant.MAPPING_TEMPLATE, XContentType.JSON);
        //创建索引库
        restHighLevelClient.indices().create(hotel, RequestOptions.DEFAULT);
    }

    /**
     * 删除酒店index（表结构）
     *
     * @throws IOException
     */
    @Test
    void deleteHotelIndex() throws IOException {
        DeleteIndexRequest hotel = new DeleteIndexRequest("hotel");
        restHighLevelClient.indices().delete(hotel,RequestOptions.DEFAULT);
    }

    /**
     * 判断指定index是否存在
     *
     * @throws IOException
     */
    @Test
    void existHotelIndex() throws IOException {
        GetIndexRequest hotel = new GetIndexRequest("hotel");
        boolean exists = restHighLevelClient.indices().exists(hotel, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
}
