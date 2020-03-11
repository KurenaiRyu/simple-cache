package com.iotlead.cache.core.redis;

import com.iotlead.cache.core.Cache;
import com.iotlead.cache.core.util.KryoUtil;
import lombok.Getter;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Jedis Cache
 *
 * @author liufuhong
 * @since 2020-03-10 15:56
 */

@Getter
public class RedisCache extends RedisCacheAbstract implements Cache {

  //TODO:

  public RedisCache(JedisPool pool) {
    super(pool);
  }

  public RedisCache(String host, int port) {
    super(host, port);
  }

  public RedisCache(String host, int port, String password) {
    super(host, port, password);
  }

  public RedisCache(String host, int port, String password, int database) {
    super(host, port, password, database);
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
    return get(jedis -> {
      List<byte[]> result = jedis.mget(keys.stream()
                                           .map(key -> buildCacheKeyBytes(key, namespace))
                                           .collect(Collectors.toList())
                                           .toArray(new byte[0][0]));
      return (List<T>) result.stream().map(KryoUtil::readFromByteArray).collect(Collectors.toList());
    });
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
    execute(jedis -> {
      List<byte[]> params = new ArrayList<>();
      keyValueMap.forEach((key, value) -> {
        params.add(buildCacheKeyBytes(key, value.getClass().getName()));
        params.add(KryoUtil.writeToByteArray(value));
      });
      jedis.mset(params.toArray(new byte[0][0]));
    });
  }

  @Override
  public <T> void multiSet(String namespace, Map<String, T> keyValueMap) {
    execute(jedis -> {
      List<byte[]> params = new ArrayList<>();
      keyValueMap.forEach((key, value) -> {
        params.add(buildCacheKeyBytes(key, namespace));
        params.add(KryoUtil.writeToByteArray(value));
      });
      jedis.mset(params.toArray(new byte[0][0]));
    });
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
    return get(jedis -> {
      List<byte[]> params = new ArrayList<>();
      keyValueMap.forEach((key, value) -> {
        params.add(buildCacheKeyBytes(key, value.getClass().getName()));
        params.add(KryoUtil.writeToByteArray(value));
      });
      return jedis.msetnx(params.toArray(new byte[0][0])) > 0;
    });
  }

  @Override
  public <T> Boolean multiSetIfAbsent(String namespace, Map<String, T> keyValueMap) {
    return get(jedis -> {
      List<byte[]> params = new ArrayList<>();
      keyValueMap.forEach((key, value) -> {
        params.add(buildCacheKeyBytes(key, namespace));
        params.add(KryoUtil.writeToByteArray(value));
      });
      return jedis.msetnx(params.toArray(new byte[0][0])) > 0;
    });
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
    return get(jedis -> {
      List<byte[]> delKeys = new ArrayList<>();
      if (keys.size() == 1) {
        String key = keys.get(0);
        namespaces.forEach(n -> delKeys.add(buildCacheKeyBytes(key, n)));
      } else if (namespaces.size() == 1) {
        String namespace = namespaces.get(0);
        keys.forEach(k -> delKeys.add(buildCacheKeyBytes(k, namespace)));
      } else {
        for (int i = 0; i < Math.min(keys.size(), namespaces.size()); i++) {
          delKeys.add(buildCacheKeyBytes(keys.get(i), namespaces.get(i)));
        }
      }
      return jedis.del(delKeys.toArray(new byte[0][0])) > 0;
    });
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
    execute(BinaryJedis::flushDB);
    return true;
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
