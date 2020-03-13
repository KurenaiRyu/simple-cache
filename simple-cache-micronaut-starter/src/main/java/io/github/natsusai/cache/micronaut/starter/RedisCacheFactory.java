package io.github.natsusai.cache.micronaut.starter;

import io.github.natsusai.cache.core.Cache;
import io.github.natsusai.cache.core.redis.lettuce.LettuceCache;
import io.github.natsusai.cache.core.redis.lettuce.LettuceClusterCache;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.micronaut.configuration.lettuce.DefaultRedisClientFactory;
import io.micronaut.configuration.lettuce.NamedRedisClientFactory;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;

import javax.inject.Named;
import javax.inject.Singleton;

/**
 * @author liufuhong
 * @since 2020-03-13 16:26
 */

@Factory
public class RedisCacheFactory {

  @Value("micronaut.application.name")
  private String appName;

  @Requires(beans = DefaultRedisClientFactory.class)
  @Singleton()
  @Named("cache")
  public Cache<RedisCommands<String, Object>> lettuce(RedisClient redisClient) {
    return new LettuceCache(appName, redisClient);
  }

  @Requires(beans = NamedRedisClientFactory.class)
  @Singleton()
  @Named("cache")
  public Cache<RedisClusterCommands<String, Object>> lettuce(RedisClusterClient redisClient) {
    return new LettuceClusterCache(appName, redisClient);
  }

}
