package io.github.natsusai.cache.micronaut.starter;

import io.github.natsusai.cache.core.Cache;
import io.github.natsusai.cache.core.redis.jedis.JedisCache;
import io.github.natsusai.cache.core.redis.jedis.ShardedJedisCache;
import io.github.natsusai.cache.core.redis.lettuce.LettuceCache;
import io.github.natsusai.cache.core.redis.lettuce.LettuceClusterCache;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ShardedJedisPool;

import javax.inject.Singleton;

/**
 * @author liufuhong
 * @since 2020-03-13 16:26
 */

@Factory
public class RedisCacheFactory {

  @Value("micronaut.application.name")
  private String appName;

  @Singleton
  @Requires(beans = RedisClient.class)
  public Cache lettuceCache(RedisClient redisClient) {
    return new LettuceCache(appName, redisClient);
  }

  @Singleton
  @Requires(beans = RedisClusterClient.class)
  public Cache lettuceClusterCache(RedisClusterClient redisClient) {
    return new LettuceClusterCache(appName, redisClient);
  }

  @Singleton
  @Requires(beans = JedisPool.class)
  public Cache jedisCache(JedisPool jedisPool) {
    return new JedisCache(appName, jedisPool);
  }

  @Singleton
  @Requires(beans = ShardedJedisPool.class)
  public Cache shardedJedisCache(ShardedJedisPool shardedJedisPool) {
    return new ShardedJedisCache(appName, shardedJedisPool);
  }

}
