package io.github.natsusai.cache.core.redis.jedis;

import io.github.natsusai.cache.core.Cache;
import io.github.natsusai.cache.core.exception.NotSupportOperationException;
import io.github.natsusai.cache.core.util.KryoUtil;
import lombok.Getter;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Jedis Cache
 *
 * @author liufuhong
 * @since 2020-03-10 15:56
 */

@Getter
public class ShardedJedisCache extends ShardedJedisCacheAbstract implements Cache<ShardedJedis> {

  //TODO: 自动装载配置


  public ShardedJedisCache(String prefix, ShardedJedisPool pool) {
    super(prefix, pool);
  }

  public ShardedJedisCache(String prefix, List<JedisShardInfo> shardInfos) {
    super(prefix, shardInfos);
  }

  public ShardedJedisCache(String prefix, List<String> hosts, List<Integer> ports) {
    super(prefix, hosts, ports);
  }

  public ShardedJedisCache(String prefix, List<String> hosts, List<Integer> ports, List<String> passwords) {
    super(prefix, hosts, ports, passwords);
  }

  @Override
  public <T> T rawGet(String key) {
    return get(jedis -> KryoUtil.readFromByteArray(key.getBytes()));
  }

  @Override
  public <T> T get(String key, String namespace) {
    return get(jedis -> KryoUtil.readFromByteArray(jedis.get(buildCacheKeyBytes(key, namespace))));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> multiGet(Collection<String> keys, String namespace) {
    List<T> result = new ArrayList<>();
    keys.forEach(key -> result.add(get(key, namespace)));
    return result;
  }

  @Override
  public <T> void rawSet(String key, T cache) {
    execute(jedis -> jedis.set(key.getBytes(), KryoUtil.writeToByteArray(cache)));
  }

  @Override
  public <T> void rawSet(String key, long ttl, T cache) {
    execute(jedis -> jedis.psetex(key.getBytes(), ttl, KryoUtil.writeToByteArray(cache)));
  }

  @Override
  public <T> void set(String key, String namespace, T cache) {
    get(jedis -> jedis.set(buildCacheKeyBytes(key, namespace), KryoUtil.writeToByteArray(cache)));
  }

  @Override
  public <T> void set(String key, String namespace, long ttl, T cache) {
    execute(jedis -> jedis.psetex(buildCacheKeyBytes(key, namespace), ttl, KryoUtil.writeToByteArray(cache)));
  }

  @Override
  public <T> void multiSet(Map<String, T> keyValueMap) {
    keyValueMap.forEach(this::set);
  }

  @Override
  public <T> void multiSet(String namespace, Map<String, T> keyValueMap) {
    keyValueMap.forEach((k, v) -> set(k, namespace, v));
  }

  @Override
  public <T> Boolean rawSetIfAbsent(String key, T cache) {
    return get(jedis -> jedis.setnx(key.getBytes(), KryoUtil.writeToByteArray(cache)) > 0);
  }

  @Override
  public <T> Boolean rawSetIfAbsent(String key, long ttl, T cache) {
    return get(jedis -> {
      jedis.setnx(key.getBytes(), KryoUtil.writeToByteArray(cache));
      jedis.pexpire(key.getBytes(), ttl);
      return true;
    });
  }

  @Override
  public <T> Boolean setIfAbsent(String key, String namespace, T cache) {
    return get(jedis -> jedis.setnx(buildCacheKeyBytes(key, namespace), KryoUtil.writeToByteArray(cache)) > 0);
  }

  @Override
  public <T> Boolean setIfAbsent(String key, String namespace, long ttl, T cache) {
    return get(jedis ->{
      byte[] bytesKey = buildCacheKeyBytes(key, namespace);
      boolean result = jedis.setnx(bytesKey, KryoUtil.writeToByteArray(cache)) > 0;
      jedis.pexpire(bytesKey, ttl);
      return result;
    });
  }

  @Override
  public <T> Boolean multiSetIfAbsent(Map<String, T> keyValueMap) {
    keyValueMap.forEach(this::setIfAbsent);
    return true;
  }

  @Override
  public <T> Boolean multiSetIfAbsent(String namespace, Map<String, T> keyValueMap) {
    throw new NotSupportOperationException();
  }

  @Override
  public <T> Boolean multiSetIfAbsent(String namespace, long ttl, Map<String, T> keyValueMap) {
    return null;
  }

  @Override
  public Boolean remove(String key, String namespace) {
    return get(jedis -> jedis.del(buildCacheKeyBytes(key, namespace)) > 0);
  }

  @Override
  public Boolean multiRemove(Collection<String> keys, String namespace) {
    execute(jedis -> keys.forEach(k -> jedis.del(buildCacheKeyBytes(k, namespace))));
    return true;
  }

  @Override
  public Boolean rawRemove(String key) {
    return get(jedis -> jedis.del(key.getBytes()) > 0);
  }

  @Override
  public Boolean clear(String namespace) {
    return get(jedis -> jedis.del(buildNamespacePatternKey(namespace).getBytes()) > 0);
  }

  @Override
  public Boolean clear() {
    throw new NotSupportOperationException();
  }

  @Override
  public ShardedJedis getClient() {
    return pool.getResource();
  }

  /**
   * 生成缓存key
   * @param key 缓存标识/id
   * @param namespace 命名空间
   * @return 缓存key
   */
  private byte[] buildCacheKeyBytes(String key, String namespace) {
    return buildCacheKey(key, namespace).getBytes();
  }
}
