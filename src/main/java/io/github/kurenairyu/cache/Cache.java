package io.github.kurenairyu.cache;

import io.github.kurenairyu.cache.exception.NotSupportOperationException;
import org.apache.commons.lang3.RandomUtils;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 缓存接口类
 *
 * @author Kurenai
 * @since 2019-08-07 13:20
 */

public interface Cache {

    long VOLATILITY_TIME = 10 * 1000L;//短存时间
    //FIXME：处理缓存短时间内大面积失效的风险

    //region get

    /**
     * 查找缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @return 返回对应缓存对象
     */
    <K, V> V get(String namespace, K key);

    /**
     * 查找缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @return 返回对应缓存对象
     */
    <K, V> CompletableFuture<V> getAsync(String namespace, K key);

    /**
     * 查找缓存，若查找不到则改由传入方法获取并添加进缓存（存在则不做更新）
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @param func      不存在时则调用该方法获取
     * @return 返回对应缓存对象
     */
    default <K, V> V get(String namespace, K key, Function<K, V> func) {

        return get(namespace, key, -1, func);
    }

    /**
     * 查找缓存，若查找不到则改由传入方法获取并添加进缓存（存在则不做更新）
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @param func      不存在时则调用该方法获取
     * @return 返回对应缓存对象
     */
    default <K, V> CompletableFuture<V> getAsync(String namespace, K key, Function<K, V> func) {

        return getAsync(namespace, key, -1, func);
    }

    /**
     * 查找缓存，若查找不到则改由回调函数获取并添加进缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @param ttl       time to live (ms)
     * @param func      回调函数
     * @return 返回对应缓存对象
     */
    @SuppressWarnings("unchecked")
    default <K, V> V get(String namespace, K key, long ttl, Function<K, V> func) {

        return (V) Optional.ofNullable(get(namespace, key))
                .orElseGet(() -> {
                    if (exists(namespace, key)) return null;
                    V result = func.apply(key);
                    if (result == null) {
                        putIfAbsent(namespace, key, null, RandomUtils.nextLong(VOLATILITY_TIME / 10, VOLATILITY_TIME));
                    } else {
                        putIfAbsent(namespace, key, result, ttl);
                    }
                    return result;
                });
    }

    /**
     * 查找缓存，若查找不到则改由回调函数获取并添加进缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @param ttl       time to live (ms)
     * @param func      回调函数
     * @return 返回对应缓存对象
     */
    default <K, V> CompletableFuture<V> getAsync(String namespace, K key, long ttl, Function<K, V> func) {
        CompletableFuture<V> future = getAsync(namespace, key);
        return future.thenApply(c -> Optional.ofNullable(c).orElseGet(() -> {
            if (exists(namespace, key)) return null;
            V result = func.apply(key);
            if (result == null) {
                putIfAbsent(namespace, key, null, RandomUtils.nextLong(VOLATILITY_TIME / 10, VOLATILITY_TIME));
            } else {
                putIfAbsent(namespace, key, result, ttl);
            }
            return result;
        }));
    }

    /**
     * 查找缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param keys      缓存标识/id集合
     * @return 返回对应缓存对象
     */
    <K, V> Map<K, V> getAll(String namespace, Collection<K> keys);

    /**
     * 查找缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param keys      缓存标识/id集合
     * @return 返回对应缓存对象
     */
    <K, V> CompletableFuture<Map<K, V>> getAllAsync(String namespace, Collection<K> keys);

    /**
     * 查找缓存，若查找不到则改由回调函数获取
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param keys      缓存标识/id集合
     * @param func      回调函数
     * @return 返回对应缓存对象
     */
    default <K, V> Map<K, V> getAll(String namespace, Collection<K> keys,
                                    Function<Collection<K>, Map<K, V>> func) {
        Map<K, V> cacheKeys = getAll(namespace, keys);
        var       missKeys  = keys.stream().filter(k -> !cacheKeys.containsKey(k)).collect(Collectors.toList());
        Optional.ofNullable(func.apply(missKeys)).filter(l -> !l.isEmpty()).ifPresent(map -> {
            putAll(namespace, map);
            cacheKeys.putAll(map);
        });

        return cacheKeys;
    }

    /**
     * 查找缓存，若查找不到则改由回调函数获取
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param keys      缓存标识/id集合
     * @param func      回调函数
     * @return 返回对应缓存对象
     */
    default <K, V> CompletableFuture<Map<K, V>> getAllAsync(String namespace, Collection<K> keys,
                                                            Function<Collection<K>, Map<K, V>> func) {
        CompletableFuture<Map<K, V>> future = getAllAsync(namespace, keys);
        return future.thenApply(cacheKeys -> {
            var missKeys = keys.stream().filter(k -> !cacheKeys.containsKey(k)).collect(Collectors.toList());
            Optional.ofNullable(func.apply(missKeys)).filter(l -> !l.isEmpty()).ifPresent(map -> {
                putAll(namespace, map);
                cacheKeys.putAll(map);
            });

            return cacheKeys;
        });
    }
    // endregion

    // region put

    /**
     * 添加缓存，其中命名空间将采用class对象的限定包名（全名）
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @param value     缓存对象
     * @return 执行结果
     */
    <K, V> void put(String namespace, K key, V value);

    /**
     * 添加缓存，其中命名空间将采用class对象的限定包名（全名）
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @param value     缓存对象
     * @return 执行结果
     */
    <K, V> CompletableFuture<String> putAsync(String namespace, K key, V value);

    /**
     * 添加缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @param value     缓存对象
     * @param time      缓存时间
     * @param timeUnit  缓存时间单位
     * @return 执行结果
     */
    default <K, V> void put(String namespace, K key, V value, long time, TimeUnit timeUnit) {
        put(namespace, key, value, timeUnit.toMillis(time));
    }

    /**
     * 添加缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @param value     缓存对象
     * @param time      缓存时间
     * @param timeUnit  缓存时间单位
     * @return 执行结果
     */
    default <K, V> CompletableFuture<String> putAsync(String namespace, K key, V value, long time, TimeUnit timeUnit) {
        return putAsync(namespace, key, value, timeUnit.toMillis(time));
    }

    /**
     * 添加缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @param value     缓存对象
     * @param ttl       time to live (ms)
     * @return 执行结果
     */
    <K, V> void put(String namespace, K key, V value, long ttl);

    /**
     * 添加缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @param value     缓存对象
     * @param ttl       time to live (ms)
     * @return 执行结果
     */
    <K, V> CompletableFuture<String> putAsync(String namespace, K key, V value, long ttl);

    /**
     * 添加缓存
     *
     * @param namespace   命名空间（类似组的概念）
     * @param keyValueMap 缓存key和对象的map
     * @param <K>         键类型
     * @param <V>         值类型
     * @return 执行结果
     */
    <K, V> void putAll(String namespace, Map<K, V> keyValueMap);

    /**
     * 添加缓存
     *
     * @param namespace   命名空间（类似组的概念）
     * @param keyValueMap 缓存key和对象的map
     * @param <K>         键类型
     * @param <V>         值类型
     * @return 执行结果
     */
    <K, V> CompletableFuture<String> putAllAsync(String namespace, Map<K, V> keyValueMap);

    /**
     * 如果不存在则添加缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存唯一标识/id
     * @param value     缓存对象
     * @return 执行结果
     */
    <K, V> boolean putIfAbsent(String namespace, K key, V value);

    /**
     * 如果不存在则添加缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存唯一标识/id
     * @param value     缓存对象
     * @return 执行结果
     */
    <K, V> CompletableFuture<Boolean> putIfAbsentAsync(String namespace, K key, V value);

    /**
     * 如果不存在则添加缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存唯一标识/id
     * @param value     缓存对象
     * @param time      缓存时间
     * @param timeUnit  缓存时间单位
     * @return 执行结果
     */
    default <K, V> boolean putIfAbsent(String namespace, K key, V value, long time, TimeUnit timeUnit) {
        return putIfAbsent(namespace, key, value, timeUnit.toMillis(time));
    }

    /**
     * 如果不存在则添加缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存唯一标识/id
     * @param value     缓存对象
     * @param time      缓存时间
     * @param timeUnit  缓存时间单位
     * @return 执行结果
     */
    default <K, V> CompletableFuture<Boolean> putIfAbsentAsync(String namespace, K key, V value, long time, TimeUnit timeUnit) {
        return putIfAbsentAsync(namespace, key, value, timeUnit.toMillis(time));
    }

    /**
     * 如果不存在则添加缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存唯一标识/id
     * @param value     缓存对象
     * @return 执行结果
     */
    <K, V> boolean putIfAbsent(String namespace, K key, V value, long ttl);

    /**
     * 如果不存在则添加缓存
     *
     * @param <K>       键类型
     * @param <V>       值类型
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存唯一标识/id
     * @param value     缓存对象
     * @return 执行结果
     */
    <K, V> CompletableFuture<Boolean> putIfAbsentAsync(String namespace, K key, V value, long ttl);

    /**
     * 如果不存在则添加缓存
     *
     * @param namespace   命名空间（类似组的概念）
     * @param keyValueMap 缓存key和对象的map<K, V>
     * @return 执行结果
     */
    <K, V> boolean putAllIfAbsent(String namespace, Map<K, V> keyValueMap);

    /**
     * 如果不存在则添加缓存
     *
     * @param namespace   命名空间（类似组的概念）
     * @param keyValueMap 缓存key和对象的map<K, V>
     * @return 执行结果
     */
    <K, V> CompletableFuture<Boolean> putAllIfAbsentAsync(String namespace, Map<K, V> keyValueMap);

    /**
     * 如果不存在则添加缓存
     *
     * @param <K>         键类型
     * @param <V>         值类型
     * @param namespace   命名空间（类似组的概念）
     * @param keyValueMap 缓存key和对象的map
     * @param time        缓存时间
     * @param timeUnit    缓存时间单位
     * @return 执行结果
     */
    default <K, V> boolean putAllIfAbsent(String namespace, Map<K, V> keyValueMap, long time, TimeUnit timeUnit) {
        return putAllIfAbsent(namespace, keyValueMap, timeUnit.toMillis(time));
    }

    /**
     * 如果不存在则添加缓存
     *
     * @param <K>         键类型
     * @param <V>         值类型
     * @param namespace   命名空间（类似组的概念）
     * @param keyValueMap 缓存key和对象的map
     * @param time        缓存时间
     * @param timeUnit    缓存时间单位
     * @return 执行结果
     */
    default <K, V> CompletableFuture<Boolean> putAllIfAbsentAsync(String namespace, Map<K, V> keyValueMap, long time, TimeUnit timeUnit) {
        return putAllIfAbsentAsync(namespace, keyValueMap, timeUnit.toMillis(time));
    }

    /**
     * 如果不存在则添加缓存
     *
     * @param <K>         键类型
     * @param <V>         值类型
     * @param namespace   命名空间（类似组的概念）
     * @param keyValueMap 缓存key和对象的map
     * @param ttl         time to live (ms)
     * @return 执行结果
     */
    <K, V> boolean putAllIfAbsent(String namespace, Map<K, V> keyValueMap, long ttl);

    /**
     * 如果不存在则添加缓存
     *
     * @param <K>         键类型
     * @param <V>         值类型
     * @param namespace   命名空间（类似组的概念）
     * @param keyValueMap 缓存key和对象的map
     * @param ttl         time to live (ms)
     * @return 执行结果
     */
    <K, V> CompletableFuture<Boolean> putAllIfAbsentAsync(String namespace, Map<K, V> keyValueMap, long ttl);

    // endregion

    // region evict

    /**
     * 移除指定缓存（非模糊匹配）
     *
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @param <K>       键类型
     * @return 执行结果
     */
    <K> boolean remove(String namespace, K key);

    /**
     * 移除指定缓存（非模糊匹配）
     *
     * @param namespace 命名空间（类似组的概念）
     * @param key       缓存标识/id
     * @param <K>       键类型
     * @return 执行结果
     */
    <K> CompletableFuture<Boolean> removeAsync(String namespace, K key);

    /**
     * 移除指定缓存（非模糊匹配）
     *
     * @param <K>       键类型
     * @param namespace 命名空间（类似组的概念）
     * @param keys      缓存标识/id
     * @return 执行结果
     */
    <K> boolean removeAll(String namespace, Collection<K> keys);

    /**
     * 移除指定缓存（非模糊匹配）
     *
     * @param <K>       键类型
     * @param namespace 命名空间（类似组的概念）
     * @param keys      缓存标识/id
     * @return 执行结果
     */
    <K> CompletableFuture<Boolean> removeAllAsync(String namespace, Collection<K> keys);

    // endregion

    // region clear

    /**
     * 清除指定命名空间所有缓存
     *
     * @param namespace 命名空间（类似组的概念）
     * @return 执行结果
     */
    boolean clear(String namespace);

    /**
     * 清除指定命名空间所有缓存
     *
     * @param namespace 命名空间（类似组的概念）
     * @return 执行结果
     */
    CompletableFuture<Boolean> clearAsync(String namespace);

    /**
     * 清除当前所有缓存
     *
     * @return 执行结果
     */
    boolean clearAll();

    /**
     * 清除当前所有缓存
     *
     * @return 执行结果
     */
    CompletableFuture<String> clearAllAsync();

    // endregion

    // region other

    /**
     * 查看指定缓存是否存在
     *
     * @param namespace 命名空间
     * @param keys      键值
     * @return
     */
    <K> boolean existsAll(String namespace, Collection<K> keys);

    /**
     * 查看指定缓存是否存在
     *
     * @param namespace 命名空间
     * @param keys      键值
     * @return
     */
    <K> CompletableFuture<Boolean> existsAllAsync(String namespace, Collection<K> keys);

    /**
     * 查看指定缓存是否存在
     *
     * @param namespace 命名空间
     * @param key       键值
     * @return 存在则为 true，否则 false
     */
    <K> boolean exists(String namespace, K key);

    /**
     * 查看指定缓存是否存在
     *
     * @param namespace 命名空间
     * @param key       键值
     * @return 存在则为 true，否则 false
     */
    <K> CompletableFuture<Boolean> existsAsync(String namespace, K key);

    /**
     * 更新过期时间
     *
     * @param namespace 命名空间
     * @param key       键值
     * @return 存在则为 true，否则 false
     */
    <K> Boolean expire(String namespace, K key, long ttl, TimeUnit timeUnit);

    /**
     * 更新过期时间
     *
     * @param namespace 命名空间
     * @param key       键值
     * @return 存在则为 true，否则 false
     */
    <K> CompletableFuture<Boolean> expireAsync(String namespace, K key, long ttl, TimeUnit timeUnit);

    /**
     * 分布式锁
     *
     * @param lockKey 锁的key
     * @param ttl     锁的有效时间(ms)
     * @return 被锁操作返回值
     * @throws NotSupportOperationException 不支持该操作时抛出异常
     */
    default Lock getLock(String lockKey, long ttl) throws Exception {
        return null;
    }

    /**
     * 获取调用客户端实例（不进行包装的客户端）
     *
     * @return 客户端实例
     */
    <T> T getExec() throws Exception;

    /**
     * 获取调用客户端实例（不进行包装的客户端）
     *
     * @return 客户端实例
     */
    <T> T getExecAsync() throws Exception;
    // endregion
}
