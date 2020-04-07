package io.github.natsusai.cache.core.redis.lettuce;

import io.github.natsusai.cache.core.Cache;
import io.github.natsusai.cache.core.util.SnowFlakeGenerator;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import lombok.Getter;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Lettuce Cache
 *
 * @author liufuhong
 * @since 2020-03-10 15:56
 */

@Getter
public class LettuceCache extends LettuceCacheAbstract<String, Object> implements Cache {

  public LettuceCache(String prefix, RedisClient redisClient) {
    super(prefix, redisClient);
  }

  public LettuceCache(String prefix, RedisClient redisClient, RedisCodec<String, Object> redisCodec) {
    super(prefix, redisClient, redisCodec);
  }

  public LettuceCache(String prefix, String host, int port) {
    super(prefix, host, port);
  }

  public LettuceCache(String prefix, String host, int port, String password) {
    super(prefix, host, port, password);
  }

  public LettuceCache(String prefix, String host, int port, String password, int database) {
    super(prefix, host, port, password, database);
  }

  public LettuceCache(String prefix, String host, int port, String password, int database, RedisCodec<String, Object> redisCodec) {
    super(prefix, host, port, password, database, redisCodec);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T rawGet(String key) {
    return (T) commands.get(key);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T  get(String key, String namespace) {
    return (T) commands.get(buildCacheKey(key, namespace));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> multiGet(Collection<String> keys, String namespace) {
    List<KeyValue<String, Object>> keyValues = commands.mget(keys.stream()
                                                            .map(key -> buildCacheKey(key, namespace))
                                                            .toArray(String[]::new));
    return (List<T>) keyValues.stream().map(KeyValue::getValue).collect(Collectors.toList());
  }

  @Override
  public <T> void rawSet(String key, T cache) {
    commands.set(key, cache);
  }

  @Override
  public <T> void rawSet(String key, long ttl, T cache) {
    commands.psetex(key, ttl, cache);
  }

  @Override
  public <T> void set(String key, String namespace, T cache) {
    commands.set(buildCacheKey(key, namespace), cache);
  }

  @Override
  public <T> void set(String key, String namespace, long ttl, T cache) {
    commands.psetex(buildCacheKey(key, namespace), ttl, cache);
  }

  @Override
  public <T> void multiSet(Map<String, T> keyValueMap) {
    Map<String, Object> params = new HashMap<>();
    keyValueMap.forEach((k, v) -> params.put(buildCacheKey(k, v.getClass().getName()), v));
    commands.mset(params);
  }

  @Override
  public <T> void multiSet(String namespace, Map<String, T> keyValueMap) {
    Map<String, Object> params = new HashMap<>();
    keyValueMap.forEach((k, v) -> params.put(buildCacheKey(k, namespace), v));
    commands.mset(params);
  }

  @Override
  public <T> Boolean rawSetIfAbsent(String key, T cache) {
    return commands.setnx(key, cache);
  }

  @Override
  public <T> Boolean rawSetIfAbsent(String key, long ttl, T cache) {
    commands.setnx(key, cache);
    return commands.pexpire(key, ttl);
  }

  @Override
  public <T> Boolean setIfAbsent(String key, String namespace, T cache) {
    return commands.setnx(buildCacheKey(key, namespace), cache);
  }

  @Override
  public <T> Boolean setIfAbsent(String key, String namespace, long ttl, T cache) {
    String buildKey = buildCacheKey(key, namespace);
    boolean result = commands.setnx(buildKey, cache);
    commands.pexpire(buildKey, ttl);
    return result;
  }

  @Override
  public <T> Boolean multiSetIfAbsent(Map<String, T> keyValueMap) {
    Map<String, Object> params = new HashMap<>();
    keyValueMap.forEach((k ,v) -> params.put(buildCacheKey(k, v.getClass().getName()), v));
    return commands.msetnx(params);
  }

  @Override
  public <T> Boolean multiSetIfAbsent(String namespace, Map<String, T> keyValueMap) {
    Map<String, Object> params = new HashMap<>();
    keyValueMap.forEach((k ,v) -> params.put(buildCacheKey(k, namespace), v));
    return commands.msetnx(params);
  }

  @Override
  public <T> Boolean multiSetIfAbsent(String namespace, long ttl, Map<String, T> keyValueMap) {
    Map<String, Object> params = new HashMap<>();
    List<String> buildKeys = new ArrayList<>();
    keyValueMap.forEach((k ,v) -> {
      String buildKey = buildCacheKey(k, namespace);
      buildKeys.add(buildKey);
      params.put(buildKey, v);
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
    return commands.del(buildNamespacePatternKey(namespace)) > 0;
  }

  @Override
  public boolean exists(String key, String namespace) {
    return commands.exists(buildCacheKey(key, namespace)) > 0;
  }

  @Override
  public boolean rawExists(String key) {
    return commands.exists(key) > 0;
  }

  @Override
  public Boolean clear() {
    commands.flushdb();
    return true;
  }

  @Override
  public <R> R lock(String lockKey, long lockValidityTimeInMilliseconds, Supplier<R> supplier) throws Exception {
    try (LettuceLock<String, Object> lock =
        new LettuceLock<>(SnowFlakeGenerator.getInstance().nextId() + "",
            lockKey,
            lockValidityTimeInMilliseconds,
            this.commands)
    ) {
      if (lock.lock()) return supplier.get();
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public RedisCommands<String, Object> getClient() {
    return commands;
  }
}
