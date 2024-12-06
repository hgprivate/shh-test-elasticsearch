package cn.shh.test.es;

import cn.shh.test.es.util.ElasticSearchUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;

public class TmpTest {
    public static void main(String[] args) {
        ElasticsearchClient elasticsearchClient = ElasticSearchUtil.elasticsearchClient();
        System.out.println("elasticsearchClient = " + elasticsearchClient);
    }
}
