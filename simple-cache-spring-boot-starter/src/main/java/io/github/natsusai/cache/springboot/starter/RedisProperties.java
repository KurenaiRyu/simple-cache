package io.github.natsusai.cache.springboot.starter;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author liufuhong
 * @since 2020-03-31 16:17
 */

//TODO: 暂时随便写的
@ConfigurationProperties("spring.redis")
public class RedisProperties {
    private String host;
    private int     port;
    private String password;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
