package io.github.natsusai.cache.core.redis.lettuce;

import io.github.natsusai.cache.core.Cache;
import io.github.natsusai.cache.core.util.KryoUtil;
import io.lettuce.core.KeyValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.Getter;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Jedis Cache
 *
 * @author liufuhong
 * @since 2020-03-10 15:56
 */

@Getter
public class LettuceCache extends LettuceCacheAbstract<String, byte[]> implements Cache<RedisCommands> {

  public LettuceCache(String prefix, StatefulRedisConnection connection) {
    super(prefix, connection);
  }

  public LettuceCache(String prefix, RedisCommands commands) {
    super(prefix, commands);
  }

  @Override
  public <T> T rawGet(String key) {
    return KryoUtil.readFromByteArray(commands.get(key));
  }

  @Override
  public <T> T get(String key, String namespace) {
    return KryoUtil.readFromByteArray(commands.get(buildCacheKey(key, namespace)));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> multiGet(Collection<String> keys, String namespace) {
    List<KeyValue<String, byte[]>> keyValues = commands.mget(keys.stream()
                                                            .map(key -> buildCacheKey(key, namespace))
                                                            .toArray(String[]::new));
    return (List<T>) keyValues.stream().map(KeyValue::getValue).map(KryoUtil::readFromByteArray).collect(Collectors.toList());
  }

  @Override
  public <T> void rawSet(String key, T cache) {
    commands.set(key, KryoUtil.writeToByteArray(cache));
  }

  @Override
  public <T> void rawSet(String key, long ttl, T cache) {
    commands.psetex(key, ttl, KryoUtil.writeToByteArray(cache));
  }

  @Override
  public <T> void set(String key, String namespace, T cache) {
    commands.set(buildCacheKey(key, namespace), KryoUtil.writeToByteArray(cache));
  }

  @Override
  public <T> void set(String key, String namespace, long ttl, T cache) {
    commands.psetex(buildCacheKey(key, namespace), ttl, KryoUtil.writeToByteArray(cache));
  }

  @Override
  public <T> void multiSet(Map<String, T> keyValueMap) {
    Map<String, byte[]> params = new HashMap<>();
    keyValueMap.forEach((k, v) -> params.put(buildCacheKey(k, v.getClass().getName()), KryoUtil.writeToByteArray(v)));
    commands.mset(params);
  }

  @Override
  public <T> void multiSet(String namespace, Map<String, T> keyValueMap) {
    Map<String, byte[]> params = new HashMap<>();
    keyValueMap.forEach((k, v) -> params.put(buildCacheKey(k, namespace), KryoUtil.writeToByteArray(v)));
    commands.mset(params);
  }

  @Override
  public <T> Boolean rawSetIfAbsent(String key, T cache) {
    return commands.setnx(key, KryoUtil.writeToByteArray(cache));
  }

  @Override
  public <T> Boolean rawSetIfAbsent(String key, long ttl, T cache) {
    commands.setnx(key, KryoUtil.writeToByteArray(cache));
    return commands.pexpire(key, ttl);
  }

  @Override
  public <T> Boolean setIfAbsent(String key, String namespace, T cache) {
    return commands.setnx(buildCacheKey(key, namespace), KryoUtil.writeToByteArray(cache));
  }

  @Override
  public <T> Boolean setIfAbsent(String key, String namespace, long ttl, T cache) {
    String buildKey = buildCacheKey(key, namespace);
    boolean result = commands.setnx(buildKey, KryoUtil.writeToByteArray(cache));
    commands.pexpire(buildKey, ttl);
    return result;
  }

  @Override
  public <T> Boolean multiSetIfAbsent(Map<String, T> keyValueMap) {
    Map<String, byte[]> params = new HashMap<>();
    keyValueMap.forEach((k ,v) -> params.put(buildCacheKey(k, v.getClass().getName()), KryoUtil.writeToByteArray(v)));
    return commands.msetnx(params);
  }

  @Override
  public <T> Boolean multiSetIfAbsent(String namespace, Map<String, T> keyValueMap) {
    Map<String, byte[]> params = new HashMap<>();
    keyValueMap.forEach((k ,v) -> params.put(buildCacheKey(k, namespace), KryoUtil.writeToByteArray(v)));
    return commands.msetnx(params);
  }

  @Override
  public <T> Boolean multiSetIfAbsent(String namespace, long ttl, Map<String, T> keyValueMap) {
    Map<String, byte[]> params = new HashMap<>();
    List<String> buildKeys = new ArrayList<>();
    keyValueMap.forEach((k ,v) -> {
      String buildKey = buildCacheKey(k, namespace);
      buildKeys.add(buildKey);
      params.put(buildKey, KryoUtil.writeToByteArray(v));
    });
    Boolean result = commands.msetnx(params);
    buildKeys.forEach(key -> commands.pexpire(key, ttl));
    return result;
  }

  @Override
  public Boolean remove(String key, String namespace) {
    return commands.del(buildCacheKey(key, namespace)) > 0;
  }

  @Override
  public Boolean multiRemove(Collection<String> keys, String namespace) {
    List<String> params = new ArrayList<>();
    keys.forEach(k -> params.add(buildCacheKey(k, namespace)));
    return commands.del(params.toArray(new String[0])) > 0;
  }

  @Override
  public Boolean rawRemove(String key) {
    return commands.del(key) > 0;
  }

  @Override
  public Boolean clear(String namespace) {
    return commands.del(buildDeleteCacheKeyPattern(namespace)) > 0;
  }

  @Override
  public Boolean clear() {
    commands.flushdb();
    return true;
  }

  @Override
  public RedisCommands getClient() {
    return commands;
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
