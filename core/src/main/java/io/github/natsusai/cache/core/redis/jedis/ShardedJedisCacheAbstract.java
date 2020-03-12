package io.github.natsusai.cache.core.redis.jedis;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import java.util.ArrayList;
import java.util.List;
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
public abstract class ShardedJedisCacheAbstract {

  protected final ShardedJedisPool pool;

  /**
   * 缓存前缀
   * <p>
   * 通常为应用名称
   * </p>
   */
  protected final String PREFIX;
  protected final String CONNECTOR = ":";

  /**
   * @param prefix 缓存键值前缀
   * @param pool 缓存池
   */
  public ShardedJedisCacheAbstract(String prefix, ShardedJedisPool pool) {
    this.PREFIX = prefix == null ? "" : prefix;
    this.pool = pool;
  }

  /**
   *
   * @param prefix 缓存键值前缀
   * @param shardInfos 节点配置信息
   */
  public ShardedJedisCacheAbstract(String prefix, List<JedisShardInfo> shardInfos) {
    this.PREFIX = prefix == null ? "" : prefix;
    this.pool = new ShardedJedisPool(new JedisPoolConfig(), shardInfos);
  }

  /**
   * 构建缓存
   * @param prefix 缓存键值前缀
   */
  public ShardedJedisCacheAbstract(String prefix, List<String> hosts, List<Integer> ports) {
    this.PREFIX = prefix == null ? "" : prefix;
    List<JedisShardInfo> shardInfos = new ArrayList<>();
    if (hosts.size() == 1) {
      String host = hosts.get(0);
      ports.forEach(port -> shardInfos.add(new JedisShardInfo(host, port)));
    } else if (ports.size() == 1) {
      Integer port = ports.get(0);
      hosts.forEach(host -> shardInfos.add(new JedisShardInfo(host, port)));
    } else {
      for (int i = 0; i < Math.min(hosts.size(), ports.size()); i++) {
        shardInfos.add(new JedisShardInfo(hosts.get(i), ports.get(i)));
      }
    }
    this.pool = new ShardedJedisPool(new JedisPoolConfig(), shardInfos);
  }

  public ShardedJedisCacheAbstract(String prefix, List<String> hosts, List<Integer> ports, List<String> passwords) {
    this.PREFIX = prefix == null ? "" : prefix;
    List<JedisShardInfo> shardInfos = new ArrayList<>();
    if (hosts.size() == 1) {
      String host = hosts.get(0);
      if (ports.size() == 1) {
        Integer port = ports.get(0);
        if (passwords.size() == 1) {
          shardInfos.add(new JedisShardInfo(host, port, passwords.get(0)));
        } else {
          passwords.forEach(password -> shardInfos.add(new JedisShardInfo(host, port, password)));
        }
      } else if (passwords.size() == 1) {
        String password = passwords.get(0);
        ports.forEach(port -> shardInfos.add(new JedisShardInfo(host, port, password)));
      } else {
        for (int i = 0; i < Math.min(ports.size(), passwords.size()); i++) {
          shardInfos.add(new JedisShardInfo(host, ports.get(i), passwords.get(i)));
        }
      }
    } else if (ports.size() == 1) {
      Integer port = ports.get(0);
      if (passwords.size() == 1) {
        String password = passwords.get(0);
        hosts.forEach(host -> shardInfos.add(new JedisShardInfo(host, port, password)));
      } else {
        for (int i = 0; i < Math.min(hosts.size(), passwords.size()); i++) {
          shardInfos.add(new JedisShardInfo(hosts.get(i), port, passwords.get(i)));
        }
      }
      hosts.forEach(host -> shardInfos.add(new JedisShardInfo(host, port)));
    } else if (passwords.size() == 1) {
      String password = passwords.get(0);
      for (int i = 0; i < Math.min(hosts.size(), ports.size()); i++) {
        shardInfos.add(new JedisShardInfo(hosts.get(i), ports.get(i), password));
      }
    } else {
      for (int i = 0; i < Math.min(hosts.size(), ports.size()); i++) {
        shardInfos.add(new JedisShardInfo(hosts.get(i), ports.get(i)));
      }
    }
    this.pool = new ShardedJedisPool(new JedisPoolConfig(), shardInfos);
  }

  /**
   * 执行传入方法并返回
   * </p>
   * @param execute 执行方法
   * @param <R> 返回类型
   * @return
   */
  protected  <R> R get(Function<ShardedJedis, R> execute) {
    try (ShardedJedis jedis = pool.getResource()) {
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
  protected void execute(Consumer<ShardedJedis> execute) {
    try (ShardedJedis jedis = pool.getResource()) {
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
