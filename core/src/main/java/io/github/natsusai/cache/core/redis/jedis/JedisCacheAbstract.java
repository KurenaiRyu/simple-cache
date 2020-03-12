package io.github.natsusai.cache.core.redis.jedis;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Jedis Cache
 *
 * @author liufuhong
 * @since 2020-03-10 15:56
 */

@Getter
public abstract class JedisCacheAbstract {

  protected final JedisPool pool;

  /**
   * 缓存前缀
   * <p>
   * 通常为应用名称
   * </p>
   */
  protected final String PREFIX;
  protected final String CONNECTOR = ":";

  public JedisCacheAbstract(String prefix, JedisPool pool) {
    this.PREFIX = prefix == null ? "" : prefix;
    this.pool = pool;
  }

  public JedisCacheAbstract(String prefix, String host, int port) {
    this.PREFIX = prefix == null ? "" : prefix;
    this.pool = new JedisPool(host, port);
  }

  public JedisCacheAbstract(String prefix, String host, int port, String password) {
    this.PREFIX = prefix == null ? "" : prefix;
    this.pool = new JedisPool(new JedisPoolConfig(), host, port, Protocol.DEFAULT_TIMEOUT, password);
  }

  public JedisCacheAbstract(String prefix, String host, int port, String password, int database) {
    this.PREFIX = prefix == null ? "" : prefix;
    this.pool = new JedisPool(new JedisPoolConfig(), host, port, Protocol.DEFAULT_TIMEOUT, password, database);
  }

  /**
   * 执行传入方法并返回
   * </p>
   * @param execute 执行方法
   * @param <R> 返回类型
   * @return
   */
  protected  <R> R get(Function<Jedis, R> execute) {
    try (Jedis jedis = pool.getResource()) {
      return execute.apply(jedis);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * 执行方法
   * </p>
   * @param execute 执行方法
   */
  protected void execute(Consumer<Jedis> execute) {
    try (Jedis jedis = pool.getResource()) {
      execute.accept(jedis);
    } catch (Exception e) {
      e.printStackTrace();
    }
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
   * 生成删除对应命名空间所有缓存的表达式
   * @param namespace 命名空间
   * @return 表达式字符串
   */
  protected String buildDeleteCacheKeyPattern(String namespace) {
    return Stream.of(PREFIX.toUpperCase(), namespace, "*")
        .filter(StringUtils::isNotBlank).collect(Collectors.joining(CONNECTOR));
  }
}
