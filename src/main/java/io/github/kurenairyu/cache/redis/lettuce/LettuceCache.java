package io.github.kurenairyu.cache.redis.lettuce;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.api.sync.RedisTransactionalCommands;
import io.lettuce.core.codec.RedisCodec;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Lettuce Cache
 *
 * @author Kurenai
 * @since 2020-03-10 15:56
 */

@Slf4j
public class LettuceCache extends LettuceCacheAbstract {

    public LettuceCache(String uri) {
        super(uri);
    }

    public LettuceCache(String host, int port) {
        super(host, port);
    }

    public LettuceCache(String host, int port, int database) {
        super(host, port, database);
    }

    public LettuceCache(String host, int port, String password, int database) {
        super(host, port, password, database);
    }

    public LettuceCache(String host, int port, String password, int database, RedisCodec<String, ?> redisCodec) {
        super(host, port, password, database, redisCodec);
    }

    public LettuceCache(RedisURI redisURI, RedisCodec<String, ?> redisCodec) {
        super(redisURI, redisCodec);
    }

    public LettuceCache(RedisURI redisURI, RedisCodec<String, ?> redisCodec, GenericObjectPoolConfig<StatefulConnection<String, ?>> poolConfig) {
        super(redisURI, redisCodec, poolConfig);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> V get(String namespace, K key) {
        return (V) execCmd(cmd -> cmd.get(buildKey(namespace, key)));
    }

    /**
     * 查找缓存
     *
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @return 返回对应缓存对象
     */
    @Override
    public <K, V> CompletableFuture<V> getAsync(String namespace, K key) {
        return execAsyncCmd(cmd -> (CompletableFuture<V>) cmd.get(buildKey(namespace, key)));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Map<K, V> getAll(String namespace, Collection<K> keys) {
        if (keys.isEmpty()) return Collections.emptyMap();
        return execCmd(cmd -> cmd.mget(keys.stream().map(k -> buildKey(namespace, k)).toArray(String[]::new)))
                .stream()
                .collect(Collectors.toMap(o -> (K) o.getKey(), o -> (V) o.getValue()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> CompletableFuture<Map<K, V>> getAllAsync(String namespace, Collection<K> keys) {
        if (keys.isEmpty()) return CompletableFuture.completedFuture(Collections.emptyMap());
        return execAsyncCmd(cmd -> (CompletableFuture<Map<K, V>>) cmd.mget(keys.stream().map(k -> buildKey(namespace, k)).toArray(String[]::new))
                .thenApply(items -> items.stream().collect(Collectors.toMap(o -> (K) o.getKey(), o -> (V) o.getValue())))
        );
    }

    @Override
    public <K, V> void put(String namespace, K key, V value) {
        execCmd(cmd -> cmd.set(buildKey(namespace, key), value));
    }

    /**
     * 添加缓存，其中命名空间将采用class对象的限定包名（全名）
     *
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @param value     缓存对象
     * @return 执行结果
     */
    @Override
    public <K, V> CompletableFuture<String> putAsync(String namespace, K key, V value) {
        return execAsyncCmd(cmd -> (CompletableFuture<String>) cmd.set(buildKey(namespace, key), value));
    }

    @Override
    public <K, V> void put(String namespace, K key, V value, long ttl) {
        execCmd(cmd -> cmd.psetex(buildKey(namespace, key), ttl, value));
    }

    /**
     * 添加缓存
     *
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @param value     缓存对象
     * @param ttl       time to live (ms)
     * @return 执行结果
     */
    @Override
    public <K, V> CompletableFuture<String> putAsync(String namespace, K key, V value, long ttl) {
        return execAsyncCmd(cmd -> (CompletableFuture<String>) cmd.psetex(buildKey(namespace, key), ttl, value));
    }

    @Override
    public <K, V> void putAll(String namespace, Map<K, V> keyValueMap) {
        var map = new HashMap<String, Object>();
        keyValueMap.forEach((k, v) -> map.put(buildKey(namespace, k), v));
        execCmd(cmd -> cmd.mset(map));
    }

    /**
     * 添加缓存
     *
     * @param namespace   命名空间（类似组的概念）
     * @param keyValueMap 缓存key和对象的map
     * @return 执行结果
     */
    @Override
    public <K, V> CompletableFuture<String> putAllAsync(String namespace, Map<K, V> keyValueMap) {
        var map = new HashMap<String, Object>();
        keyValueMap.forEach((k, v) -> map.put(buildKey(namespace, k), v));
        return execAsyncCmd(cmd -> (CompletableFuture<String>) cmd.mset(map));
    }

    @Override
    public <K, V> boolean putIfAbsent(String namespace, K key, V value) {
        return execCmd(cmd -> cmd.setnx(buildKey(namespace, key), value));
    }

    /**
     * 如果不存在则添加缓存
     *
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存唯一标识/id
     * @param value     缓存对象
     * @return 执行结果
     */
    @Override
    public <K, V> CompletableFuture<Boolean> putIfAbsentAsync(String namespace, K key, V value) {
        return execAsyncCmd(cmd -> (CompletableFuture<Boolean>) cmd.setnx(buildKey(namespace, key), value));
    }

    @Override
    public <K, V> boolean putIfAbsent(String namespace, K key, V value, long ttl) {
        final var redisKey = buildKey(namespace, key);
        return execCmd(cmd -> {
            if (cmd instanceof RedisTransactionalCommands) {
                try {
                    ((RedisTransactionalCommands<?, ?>) cmd).multi();
                    if (Boolean.FALSE.equals(cmd.setnx(redisKey, value)) || Boolean.FALSE.equals(cmd.expire(redisKey, ttl))) {
                        ((RedisTransactionalCommands<?, ?>) cmd).discard();
                        return false;
                    }
                    ((RedisTransactionalCommands<?, ?>) cmd).exec();
                } catch (Exception e) {
                    ((RedisTransactionalCommands<?, ?>) cmd).discard();
                    throw e;
                }
            } else {
                return !Boolean.FALSE.equals(cmd.setnx(redisKey, value)) && !Boolean.FALSE.equals(cmd.expire(redisKey, ttl));
            }
            return true;
        });
    }

    /**
     * 如果不存在则添加缓存
     *
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存唯一标识/id
     * @param value     缓存对象
     * @param ttl
     * @return 执行结果
     */
    @Override
    public <K, V> CompletableFuture<Boolean> putIfAbsentAsync(String namespace, K key, V value, long ttl) {
        final var redisKey = buildKey(namespace, key);
        return execAsyncCmd(cmd -> {
            if (cmd instanceof RedisTransactionalAsyncCommands) {
                return (CompletableFuture<Boolean>) ((RedisTransactionalAsyncCommands<?, ?>) cmd).multi().thenCompose(ret -> cmd.setnx(redisKey, value)
                        .thenCombine(
                                cmd.expire(redisKey, ttl),
                                (setnx, expire) -> {
                                    if (Boolean.FALSE.equals(setnx) || Boolean.FALSE.equals(expire)) {
                                        ((RedisTransactionalCommands<?, ?>) cmd).discard();
                                        return false;
                                    } else {
                                        return true;
                                    }
                                }
                        )).handle((ret, ex) -> {
                    if (ex == null) {
                        return ((RedisTransactionalAsyncCommands<?, ?>) cmd).exec().thenApply(r -> ret);
                    } else {
                        return ((RedisTransactionalAsyncCommands<?, ?>) cmd).discard().thenCompose(r -> CompletableFuture.failedFuture(ex));
                    }
                }).thenCompose(f -> f);
            } else {
                return (CompletableFuture<Boolean>) cmd.setnx(redisKey, value)
                        .thenCombine(
                                cmd.expire(redisKey, ttl),
                                (setnx, expire) -> !Boolean.FALSE.equals(setnx) || !Boolean.FALSE.equals(expire)
                        );
            }
        });
    }

    @Override
    public <K, V> boolean putAllIfAbsent(String namespace, Map<K, V> keyValueMap) {
        var map = new HashMap<String, Object>();
        keyValueMap.forEach((k, v) -> map.put(buildKey(namespace, k), v));
        return execCmd(cmd -> cmd.msetnx(map));
    }

    /**
     * 如果不存在则添加缓存
     *
     * @param namespace   命名空间（类似组的概念）
     * @param keyValueMap 缓存key和对象的map<K, V>
     * @return 执行结果
     */
    @Override
    public <K, V> CompletableFuture<Boolean> putAllIfAbsentAsync(String namespace, Map<K, V> keyValueMap) {
        var map = new HashMap<String, Object>();
        keyValueMap.forEach((k, v) -> map.put(buildKey(namespace, k), v));
        return execAsyncCmd(cmd -> (CompletableFuture<Boolean>) cmd.msetnx(map));
    }

    @Override
    public <K, V> boolean putAllIfAbsent(String namespace, Map<K, V> keyValueMap, long ttl) {
        var map = new HashMap<String, Object>();
        keyValueMap.forEach((k, v) -> map.put(buildKey(namespace, k), v));
        return execCmd(cmd -> {
            if (cmd instanceof RedisTransactionalCommands) {
                try {
                    ((RedisTransactionalCommands<?, ?>) cmd).multi();
                    if (!cmd.msetnx(map)) {
                        ((RedisTransactionalCommands<?, ?>) cmd).discard();
                        return false;
                    }
                    map.keySet().forEach(k -> cmd.pexpire(k, ttl));
                    ((RedisTransactionalCommands<?, ?>) cmd).exec();
                } catch (Exception e) {
                    ((RedisTransactionalCommands<?, ?>) cmd).discard();
                }
            } else {
                cmd.msetnx(map);
                map.keySet().forEach(k -> cmd.pexpire(k, ttl));
            }
            return true;
        });
    }

    /**
     * 如果不存在则添加缓存
     *
     * @param namespace   命名空间（类似组的概念）
     * @param keyValueMap 缓存key和对象的map
     * @param ttl         time to live (ms)
     * @return 执行结果
     */
    @Override
    public <K, V> CompletableFuture<Boolean> putAllIfAbsentAsync(String namespace, Map<K, V> keyValueMap, long ttl) {
        var map = new HashMap<String, Object>();
        keyValueMap.forEach((k, v) -> map.put(buildKey(namespace, k), v));
        return execAsyncCmd(cmd -> {
            if (cmd instanceof RedisTransactionalAsyncCommands) {
                return (CompletableFuture<Boolean>) ((RedisTransactionalAsyncCommands<?, ?>) cmd).multi().thenCompose(ret -> {
                    var futures = new ArrayList<RedisFuture<Boolean>>();
                    map.keySet().forEach(k -> futures.add(cmd.pexpire(k, ttl)));
                    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).thenApply(r -> true);
                }).thenCombine(cmd.msetnx(map), (a, b) -> {
                    if (Boolean.FALSE.equals(a) || Boolean.FALSE.equals(b)) {
                        ((RedisTransactionalCommands<?, ?>) cmd).discard();
                        return false;
                    } else {
                        return true;
                    }
                }).handle((ret, ex) -> {
                    if (ex == null) {
                        return ((RedisTransactionalAsyncCommands<?, ?>) cmd).exec().thenApply(r -> ret);
                    } else {
                        return ((RedisTransactionalAsyncCommands<?, ?>) cmd).discard().thenCompose(r -> CompletableFuture.failedFuture(ex));
                    }
                }).thenCompose(f -> f);
            } else {
                var futures = new ArrayList<RedisFuture<Boolean>>();
                map.keySet().forEach(k -> futures.add(cmd.pexpire(k, ttl)));
                return (CompletableFuture<Boolean>) CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                        .thenApply(r -> true).thenCombine(cmd.msetnx(map), (a, b) -> {
                            if (Boolean.FALSE.equals(a) || Boolean.FALSE.equals(b)) {
                                ((RedisTransactionalCommands<?, ?>) cmd).discard();
                                return false;
                            } else {
                                return true;
                            }
                        }).handle((ret, ex) -> {
                            if (ex == null) {
                                return ((RedisTransactionalAsyncCommands<?, ?>) cmd).exec().thenApply(r -> ret);
                            } else {
                                return ((RedisTransactionalAsyncCommands<?, ?>) cmd).discard().thenCompose(r -> CompletableFuture.failedFuture(ex));
                            }
                        }).thenCompose(f -> f);
            }
        });
    }

    @Override
    public <K> boolean remove(String namespace, K key) {
        return execCmd(cmd -> cmd.del(buildKey(namespace, key)) > 0);
    }

    /**
     * 移除指定缓存（非模糊匹配）
     *
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @return 执行结果
     */
    @Override
    public <K> CompletableFuture<Boolean> removeAsync(String namespace, K key) {
        return execAsyncCmd(cmd -> (CompletableFuture<Boolean>) cmd.del(buildKey(namespace, key)).thenApply(r -> r > 0));
    }

    @Override
    public final <K> boolean removeAll(String namespace, Collection<K> keys) {
        var redisKeys = keys.stream().map(key -> buildKey(namespace, key)).toArray(String[]::new);
        return execCmd(cmd -> cmd.del(redisKeys) > 0);
    }

    /**
     * 移除指定缓存（非模糊匹配）
     *
     * @param namespace 命名空间（类似组的概念）
     * @param keys      缓存标识/id
     * @return 执行结果
     */
    @Override
    public <K> CompletableFuture<Boolean> removeAllAsync(String namespace, Collection<K> keys) {
        var redisKeys = keys.stream().map(key -> buildKey(namespace, key)).toArray(String[]::new);
        return execAsyncCmd(cmd -> (CompletableFuture<Boolean>) cmd.del(redisKeys).thenApply(r -> r > 0));
    }

    @Override
    public final <K> boolean existsAll(String namespace, Collection<K> keys) {
        return execCmd(cmd -> cmd.exists(buildKeys(namespace, keys)) > 0);
    }

    /**
     * 查看指定缓存是否存在
     *
     * @param namespace 命名空间
     * @param keys      键值
     * @return
     */
    @Override
    public <K> CompletableFuture<Boolean> existsAllAsync(String namespace, Collection<K> keys) {
        return execAsyncCmd(cmd -> (CompletableFuture<Boolean>) cmd.exists(buildKeys(namespace, keys)).thenApply(r -> r > 0));
    }

    @Override
    public final <K> boolean exists(String namespace, K key) {
        return execCmd(cmd -> cmd.exists(buildKey(namespace, key)) > 0);
    }

    /**
     * 查看指定缓存是否存在
     *
     * @param namespace 命名空间
     * @param key       键值
     * @return 存在则为 true，否则 false
     */
    @Override
    public <K> CompletableFuture<Boolean> existsAsync(String namespace, K key) {
        return execAsyncCmd(cmd -> (CompletableFuture<Boolean>) cmd.exists(buildKey(namespace, key)).thenApply(count -> count > 0));
    }

    @Override
    public boolean clear(String namespace) {
        return execCmd(cmd -> cmd.del(cmd.keys(buildNamespacePatternKey(namespace)).toArray(new String[0])) > 0);
    }

    /**
     * 清除指定命名空间所有缓存
     *
     * @param namespace 命名空间（类似组的概念）
     * @return 执行结果
     */
    @Override
    public CompletableFuture<Boolean> clearAsync(String namespace) {
        return execAsyncCmd(cmd -> (CompletableFuture<Boolean>) cmd.keys(buildNamespacePatternKey(namespace))
                .thenCompose(keys -> cmd.del(keys.toArray(new String[0])))
                .thenApply(r -> r > 0));
    }

    /**
     * 清除当前库所有缓存
     */
    @Override
    public boolean clearAll() {
        execCmd(RedisServerCommands::flushdb);
        return true;
    }

    /**
     * 清除当前所有缓存
     *
     * @return 执行结果
     */
    @Override
    public CompletableFuture<String> clearAllAsync() {
        return execAsyncCmd(cmd -> (CompletableFuture<String>) cmd.flushdb());
    }

    /**
     * 获取调用客户端实例（不进行包装的客户端）
     *
     * @return 客户端实例
     */
    @Override
    public <T> T getExec() throws Exception {
        return (T) sync(connect());
    }

    /**
     * 获取调用客户端实例（不进行包装的客户端）
     *
     * @return 客户端实例
     */
    @Override
    public <T> T getExecAsync() throws Exception {
        return (T) async(connect());
    }

    @Override
    public boolean isCluster() {
        return false;
    }
}
