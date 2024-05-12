package cn.shh.test.es.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;

public class ElasticSearchUtil {
    private static final String serverUrl = "https://localhost:9200";
    private static final String password = "QV3=bXM8JBf0ifdnTYaF";
    private static final String apiKey = "VkRpckhZOEJTYWNrcHBHSklTU2g6WTRvenplbUFUX082OXRwMGhHYXUyZw==";
    private static final String httpcacrt = "/Users/shh/Java/cache/certs/es/certs/cluster-ca.p12";
    private static final String httpp12 = "/Users/shh/Java/cache/certs/es/certs/http.p12";
    private static final String transportp12 = "/Users/shh/Java/cache/certs/es/certs/transport.p12";
    public static ElasticsearchClient elasticsearchClient(){
        RestClient restClient = RestClient.builder(new HttpHost("192.168.0.10", 9200, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                    try {
                        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(AuthScope.ANY,
                                new UsernamePasswordCredentials("elastic", password));
                        Path path = Paths.get(httpcacrt);
                        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
                        Certificate certificate = certificateFactory.generateCertificate(Files.newInputStream(path));


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

        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        ElasticsearchClient esClient = new ElasticsearchClient(transport);
        return esClient;
    }

}
