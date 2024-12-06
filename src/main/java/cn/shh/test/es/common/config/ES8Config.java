package cn.shh.test.es.common.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.RequiredArgsConstructor;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;

@RequiredArgsConstructor
//@EnableConfigurationProperties(ESProperties.class)
//@Configuration
public class ES8Config {
    private final String httpcacrt = "/Users/shh/Java/cache/certs/es/http_ca.crt";
    private final ESProperties esProperties;
    @Bean
    public RestClient restClient(){
        RestClient restClient = RestClient.builder(new HttpHost(esProperties.getAddress(), esProperties.getPort(), "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                    try {
                        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        // elastic / oI_awOKMD4jA2v7HqFIv
                        credentialsProvider.setCredentials(AuthScope.ANY,
                                new UsernamePasswordCredentials(esProperties.getUsername(), esProperties.getPassword()));
                        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
                        // 方式1
                        //Certificate certificate = certificateFactory.generateCertificate(Files.newInputStream(Paths.get(httpcacrt)));
                        // 方式2
                        InputStream is = getClass().getClassLoader().getResourceAsStream("certs/vm/elasticsearch-ca.pem");
                        Certificate certificate = certificateFactory.generateCertificate(is);


                        KeyStore keyStore = KeyStore.getInstance("pkcs12");
                        keyStore.load(null, null);
                        keyStore.setCertificateEntry("ca", certificate);
                        SSLContext sslContext = SSLContexts.custom()
                                .loadTrustMaterial(keyStore, null).build();

                        httpAsyncClientBuilder.setSSLContext(sslContext);
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        return httpAsyncClientBuilder;
                    } catch (KeyStoreException | CertificateException | IOException | NoSuchAlgorithmException |
                             KeyManagementException e) {
                        throw new RuntimeException(e);
                    }
                })
                .build();
        return restClient;
    }
    @Bean
    public ElasticsearchTransport elasticsearchTransport(){
        ElasticsearchTransport transport = new RestClientTransport(restClient(), new JacksonJsonpMapper());
        return transport;
    }
    @Bean
    public ElasticsearchClient elasticsearchClient(){
        ElasticsearchClient esClient = new ElasticsearchClient(elasticsearchTransport());
        return esClient;
    }
}