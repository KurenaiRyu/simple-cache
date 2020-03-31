package io.github.natsusai.cache.springboot.starter;

import io.github.natsusai.cache.core.Cache;
import io.github.natsusai.cache.core.redis.jedis.JedisCache;
import io.github.natsusai.cache.core.redis.jedis.ShardedJedisCache;
import io.github.natsusai.cache.core.redis.lettuce.LettuceCache;
import io.github.natsusai.cache.core.redis.lettuce.LettuceClusterCache;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ShardedJedisPool;

/**
 * Redis Cache Auto Configure
 *
 * @author liufuhong
 * @since 2020-03-31 15:54
 */

@Configuration
@ConditionalOnMissingBean(Cache.class)
@EnableConfigurationProperties(RedisProperties.class)
public class RedisCacheAutoConfigure {

  @Value("spring.application.name")
  private String appName;

  @Bean
  @ConditionalOnMissingBean(RedisClient.class)
  public RedisClient redisClient(RedisProperties redisProperties) {
    return RedisClient.create(RedisURI.builder()
                                      .withHost(redisProperties.getHost())
                                      .withPort(redisProperties.getPort())
                                      .withPassword(redisProperties.getPassword()).build()
    );
  }

  @Bean
  @ConditionalOnBean(RedisClient.class)
  public Cache lettuceCache(RedisClient redisClient) {
    return new LettuceCache(appName, redisClient);
  }

  @Bean
  @ConditionalOnBean(RedisClusterClient.class)
  public Cache lettuceClusterCache(RedisClusterClient redisClient) {
    return new LettuceClusterCache(appName, redisClient);
  }

  @Bean
  @ConditionalOnBean(JedisPool.class)
  public Cache jedisCache(JedisPool jedisPool) {
    return new JedisCache(appName, jedisPool);
  }

  @Bean
  @ConditionalOnBean(ShardedJedisPool.class)
  public Cache shardedJedisCache(ShardedJedisPool shardedJedisPool) {
    return new ShardedJedisCache(appName, shardedJedisPool);
  }


}
