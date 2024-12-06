package cn.shh.test.es.common.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "es")
public class ESProperties {
    private String address;
    private Integer port;
    private String username;
    private String password;
}