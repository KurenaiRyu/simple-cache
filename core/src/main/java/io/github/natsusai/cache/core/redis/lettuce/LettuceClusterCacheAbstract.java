package io.github.natsusai.cache.core.redis.lettuce;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Jedis Cache
 *
 * @param <K>
 * @author liufuhong
 * @since 2020-03-10 15:56
 */

@Getter
public abstract class LettuceClusterCacheAbstract<K, V> {

  protected final RedisClusterCommands<K, V> commands;
  protected RedisClusterClient redisClusterClient;
  protected StatefulRedisClusterConnection<K, V> connection;

  /**
   * 缓存前缀
   * <p>
   * 通常为应用名称
   * </p>
   */
  protected final String PREFIX;
  protected final String CONNECTOR = ":";

  public LettuceClusterCacheAbstract(String prefix, RedisClusterClient redisClusterClient) {
    this.PREFIX = prefix == null ? "" : prefix;
    this.redisClusterClient = redisClusterClient;
    this.connection = this.redisClusterClient.connect(new KryoCodec<>());
    this.commands = this.connection.sync();
  }

  @SuppressWarnings("unchecked")
  public LettuceClusterCacheAbstract(String prefix, String host, int port) {
    this.PREFIX = prefix == null ? "" : prefix;
    this.commands = (RedisClusterCommands<K, V>) RedisClusterClient.create(RedisURI.builder()
                                                                 .withHost(host)
                                                                 .withPort(port)
                                                                 .build())
        .connect(new KryoCodec()).sync();
  }

  @SuppressWarnings("unchecked")
  public LettuceClusterCacheAbstract(String prefix, List<RedisURI> redisURIs) {
    this.PREFIX = prefix == null ? "" : prefix;
    this.commands = (RedisClusterCommands<K, V>) RedisClusterClient.create(redisURIs).connect(new KryoCodec()).sync();
  }

  /**
   * 生成缓存key
   * @param key 缓存标识/id
   * @param namespace 命名空间
   * @return 缓存key
   */
  protected String buildCacheKey(String key, String namespace) {
    return Stream.of(PREFIX.toUpperCase(), namespace, key)
        .filter(StringUtils::isNotBlank).collect(Collectors.joining(CONNECTOR));
  }

  /**
   * 生成命名空间所有缓存的表达式（模糊查询）
   * @param namespace 命名空间
   * @return 表达式字符串
   */
  protected String buildNamespacePatternKey(String namespace) {
    return Stream.of(PREFIX.toUpperCase(), namespace, "*")
        .filter(StringUtils::isNotBlank).collect(Collectors.joining(CONNECTOR));
  }
}
