package io.github.natsusai.cache.core.redis.lettuce;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;

/**
 * Lettuce Sentinel Cache
 *
 * @author liufuhong
 * @since 2020-03-30 11:05
 */

public class LettuceSentinelCache extends LettuceCache {

  public LettuceSentinelCache(String prefix, RedisClient redisClient) {
    super(prefix, redisClient);
  }

  public LettuceSentinelCache(String prefix, String masterId, String host, int port) {
    super(prefix, RedisClient.create(RedisURI.builder()
                                         .withSentinel(host, port)
                                         .withSentinelMasterId(masterId)
                                         .build()));
  }

  public LettuceSentinelCache(String prefix, String masterId, String host, int port, String password) {
    super(prefix, RedisClient.create(RedisURI.builder()
                                         .withSentinel(host, port)
                                         .withSentinelMasterId(masterId)
                                         .withPassword(password)
                                         .build()));
  }

  public LettuceSentinelCache(String prefix, String masterId, String host, int port, String password, int database) {
    super(prefix, RedisClient.create(RedisURI.builder()
                                         .withSentinel(host, port)
                                         .withSentinelMasterId(masterId)
                                         .withPassword(password)
                                         .withDatabase(database)
                                         .build()));
  }

  public LettuceSentinelCache(String prefix, String masterId, String host, int port, String password, int database, RedisCodec<String, Object> redisCodec) {
    super(prefix, RedisClient.create(RedisURI.builder()
                                         .withSentinel(host, port)
                                         .withSentinelMasterId(masterId)
                                         .withPassword(password)
                                         .withDatabase(database)
                                         .build()), redisCodec);
  }
}
