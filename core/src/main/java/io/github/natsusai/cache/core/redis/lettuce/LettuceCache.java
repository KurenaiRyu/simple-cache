package io.github.natsusai.cache.core.redis.lettuce;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.api.sync.RedisTransactionalCommands;
import io.lettuce.core.codec.RedisCodec;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.*;

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

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> List<V> getAll(String namespace, Collection<K> keys) {
        if (keys.isEmpty()) return Collections.emptyList();
        return (List<V>) execCmd(cmd -> cmd.mget(keys.stream().map(k -> buildKey(namespace, k)).toArray(String[]::new)));
    }

    @Override
    public <K, V> void put(String namespace, K key, V value) {
        execCmd(cmd -> cmd.set(buildKey(namespace, key), value));
    }

    @Override
    public <K, V> void put(String namespace, K key, V value, long ttl) {
        execCmd(cmd -> cmd.psetex(buildKey(namespace, key), ttl, value));
    }

    @Override
    public <K, V> void putAll(String namespace, Map<K, V> keyValueMap) {
        var map = new HashMap<String, Object>();
        keyValueMap.forEach((k, v) -> map.put(buildKey(namespace, k), v));
        execCmd(cmd -> cmd.mset(map));
    }

    @Override
    public <K, V> boolean putIfAbsent(String namespace, K key, V value) {
        return execCmd(cmd -> cmd.setnx(buildKey(namespace, key), value));
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

    @Override
    public <K, V> boolean putAllIfAbsent(String namespace, Map<K, V> keyValueMap) {
        var map = new HashMap<String, Object>();
        keyValueMap.forEach((k, v) -> map.put(buildKey(namespace, k), v));
        return execCmd(cmd -> cmd.msetnx(map));
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

    @Override
    public <K> boolean remove(String namespace, K key) {
        return execCmd(cmd -> cmd.del(buildKey(namespace, key)) > 0);
    }

    @Override
    public final <K> boolean removeAll(String namespace, Collection<K> keys) {
        var redisKeys = keys.stream().map(key -> buildKey(namespace, key)).toArray(String[]::new);
        return execCmd(cmd -> cmd.del(redisKeys) > 0);
    }

    @Override
    public final <K> boolean existsAll(String namespace, Collection<K> keys) {
        return execCmd(cmd -> cmd.exists(buildKeys(namespace, keys)) > 0);
    }

    @Override
    public final <K> boolean exists(String namespace, K key) {
        return execCmd(cmd -> cmd.exists(buildKey(namespace, key)) > 0);
    }

    @Override
    public boolean clear(String namespace) {
        return execCmd(cmd -> cmd.del(cmd.keys(buildNamespacePatternKey(namespace)).toArray(new String[0])) > 0);
    }

    /**
     * 清除当前库所有缓存
     */
    @Override
    public boolean clearAll() {
        execCmd(RedisServerCommands::flushdb);
        return true;
    }

    @Override
    public <T> T getExec() {
        return null;
    }

    @Override
    public boolean isCluster() {
        return false;
    }
}
