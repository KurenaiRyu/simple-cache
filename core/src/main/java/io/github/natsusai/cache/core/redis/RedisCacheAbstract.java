package io.github.natsusai.cache.core.redis;

import java.util.Collection;

import static io.github.natsusai.cache.core.utils.StringPool.COLON;
import static io.github.natsusai.cache.core.utils.StringPool.STAR;

/**
 * @author Kurenai
 * @since 2021-07-21 10:22
 */
public abstract class RedisCacheAbstract implements RedisCache{

    protected final String                                           CONNECTOR = COLON;

    /**
     * 生成命名空间所有缓存的表达式（模糊查询）
     *
     * @param namespace 命名空间
     * @return 表达式字符串
     */
    protected String buildNamespacePatternKey(String namespace) {
        return String.join(CONNECTOR, namespace, STAR);
    }

    /**
     * 构建redis键值
     *
     * @param namespace 命名空间
     * @param key       缓存标识/id
     * @return redis键值
     */
    protected <K> String buildKey(String namespace, K key) {
        return String.join(CONNECTOR, namespace, String.valueOf(key));
    }

    /**
     * 构建redis键值
     *
     * @param namespace 命名空间
     * @param keys 键值集合
     * @param <K> 键值类型
     * @return redis键值集合
     */
    protected <K>  String[] buildKeys(String namespace, Collection<K> keys) {
        return keys.stream().map(key -> buildKey(namespace, key)).toArray(String[]::new);
    }

}
