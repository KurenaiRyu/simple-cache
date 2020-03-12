package io.github.natsusai.cache.core.redis.jedis;

import io.github.natsusai.cache.core.Cache;
import io.github.natsusai.cache.core.exception.NotSupportOperationException;
import io.github.natsusai.cache.core.util.KryoUtil;
import lombok.Getter;
import redis.clients.jedis.JedisShardInfo;
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
public class JedisShardShardCache extends JedisShardCacheAbstract implements Cache {

  //TODO: 自动装载配置


  public JedisShardShardCache(String prefix, ShardedJedisPool pool) {
    super(prefix, pool);
  }

  public JedisShardShardCache(String prefix, List<JedisShardInfo> shardInfos) {
    super(prefix, shardInfos);
  }

  public JedisShardShardCache(String prefix, List<String> hosts, List<Integer> ports) {
    super(prefix, hosts, ports);
  }

  public JedisShardShardCache(String prefix, List<String> hosts, List<Integer> ports, List<String> passwords) {
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
    return null;
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
  public Boolean multiRemove(List<String> keys, List<String> namespaces) {
    throw new NotSupportOperationException();
  }

  @Override
  public Boolean rawRemove(String key) {
    return get(jedis -> jedis.del(key.getBytes()) > 0);
  }

  @Override
  public Boolean clear(String namespace) {
    return get(jedis -> jedis.del(buildDeleteCacheKeyPattern(namespace).getBytes()) > 0);
  }

  @Override
  public Boolean clear() {
    throw new NotSupportOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getClient() {
    return (T) pool.getResource();
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
