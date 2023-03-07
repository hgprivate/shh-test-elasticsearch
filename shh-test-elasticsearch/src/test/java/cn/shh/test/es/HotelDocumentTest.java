package cn.shh.test.es;

import cn.shh.test.es.pojo.Hotel;
import cn.shh.test.es.pojo.HotelDoc;
import cn.shh.test.es.service.IHotelService;
import cn.shh.test.es.service.impl.HotelServiceImpl;
import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@SpringBootTest
public class HotelDocumentTest {
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private IHotelService iHotelService;

    @Test
    void testInit() {
        System.out.println(this.restHighLevelClient);
    }

    @Test
    void createHotelIndex() throws IOException {
        Hotel hotel = new Hotel();
        HotelDoc hotelDoc = new HotelDoc(hotel);
        // 1.准备Request对象
        IndexRequest hotelIndex = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        // 2.准备Json文档
        hotelIndex.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        // 3.发送请求
        restHighLevelClient.index(hotelIndex, RequestOptions.DEFAULT);
    }

    @Test
    public void testAddDocument(){
        try {
            Hotel hotel = new Hotel();
            HotelDoc hotelDoc = new HotelDoc(hotel);
            IndexRequest request = new IndexRequest("hotel").id("61083");
            request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            restHighLevelClient.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void getDocumentById() throws IOException {
        // 1.准备Request
        GetRequest hotel = new GetRequest("hotel", "61083");
        // 2.发送请求，得到响应
        GetResponse hotelResponse = restHighLevelClient.get(hotel, RequestOptions.DEFAULT);
        // 3.解析响应结果
        String hotelDocSourceAsString = hotelResponse.getSourceAsString();
        // 4.json转实体类
        HotelDoc hotelDoc = JSON.parseObject(hotelDocSourceAsString, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    @Test
    void delDocumentById() throws IOException {
        DeleteRequest hotel = new DeleteRequest("hotel", "61083");
        restHighLevelClient.delete(hotel, RequestOptions.DEFAULT);
    }

    @Test
    void updateDocument() throws IOException {
        // 1.准备Request
        UpdateRequest request = new UpdateRequest("hotel", "61083");
        // 2.准备请求参数
        request.doc(
                "price", "952",
                "starName", "四钻"
        );
        // 3.发送请求
        restHighLevelClient.update(request, RequestOptions.DEFAULT);
    }

    @Test
    void bulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        List<Hotel> hotelList = iHotelService.list();
        hotelList.forEach(item -> {
            HotelDoc hotelDoc = new HotelDoc(item);
            bulkRequest.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc), XContentType.JSON));
        });
        restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
    }
}
