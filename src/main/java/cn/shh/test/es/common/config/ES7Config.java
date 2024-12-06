package cn.shh.test.es.common.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.RequiredArgsConstructor;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@EnableConfigurationProperties(ESProperties.class)
@Configuration
public class ES7Config {
    private final String httpcacrt = "/Users/shh/Java/cache/certs/es/http_ca.crt";
    @Autowired
    private ESProperties esProperties;

    @Bean
    public RestClient restClient(){
        return RestClient.builder(new HttpHost(esProperties.getAddress(), esProperties.getPort())).build();
    }
    @Bean
    public ElasticsearchTransport elasticsearchTransport(){
        return new RestClientTransport(restClient(), new JacksonJsonpMapper());
    }
    @Bean
    public ElasticsearchClient elasticsearchClient(){
        return new ElasticsearchClient(elasticsearchTransport());
    }
}