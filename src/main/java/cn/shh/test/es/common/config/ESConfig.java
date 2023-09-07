package cn.shh.test.es.common.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@EnableConfigurationProperties(ESProperties.class)
@Configuration
public class ESConfig {
    @Bean
    public RestClient restClient(ESProperties properties){
        return RestClient.builder(new HttpHost(properties.getAddress(), properties.getPort())).build();
    }
    @Bean
    public ElasticsearchTransport elasticsearchTransport(RestClient restClient){
        return new RestClientTransport(restClient, new JacksonJsonpMapper());
    }
    @Bean
    public ElasticsearchClient elasticsearchClient(ElasticsearchTransport elasticsearchTransport){
        return new ElasticsearchClient(elasticsearchTransport);
    }
}
