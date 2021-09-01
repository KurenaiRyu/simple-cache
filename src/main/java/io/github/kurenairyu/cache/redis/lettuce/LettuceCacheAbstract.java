package io.github.kurenairyu.cache.redis.lettuce;

import io.github.kurenairyu.cache.redis.RedisCacheAbstract;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.function.Function;

import static io.github.kurenairyu.cache.util.StringPool.COLON;

/**
 * Lettuce Cache Abstract
 *
 * @author Kurenai
 * @since 2020-03-10 15:56
 */
@Slf4j
public abstract class LettuceCacheAbstract extends RedisCacheAbstract {

    protected final GenericObjectPool<StatefulConnection<String, ?>> POOL;
    protected final String                                           CONNECTOR = COLON;

    public LettuceCacheAbstract(String uri) {
        this(RedisURI.create(uri), new KryoCodec<>());
    }

    public LettuceCacheAbstract(String host, int port) {
        this(host, port, 0);
    }

    public LettuceCacheAbstract(String host, int port, int database) {
        this(RedisURI.builder()
                .withHost(host)
                .withPort(port)
                .withDatabase(database)
                .build(), new KryoCodec<>());
    }

    public LettuceCacheAbstract(String host, int port, String password, int database) {
        this(host, port, password, database, new KryoCodec<>());
    }

    public LettuceCacheAbstract(String host, int port, String password, int database, RedisCodec<String, ?> redisCodec) {
        this(RedisURI.builder()
                .withHost(host)
                .withPort(port)
                .withPassword(password)
                .withDatabase(database)
                .build(), redisCodec);
    }

    public LettuceCacheAbstract(RedisURI redisURI, RedisCodec<String, ?> redisCodec) {
        this(redisURI, redisCodec, null);
    }

    public LettuceCacheAbstract(RedisURI redisURI, RedisCodec<String, ?> redisCodec, GenericObjectPoolConfig<StatefulConnection<String, ?>> poolConfig) {
        this.POOL = ConnectionPoolSupport.createGenericObjectPool(() -> {
            if (isCluster()) {
                return RedisClusterClient.create(redisURI).connect(redisCodec);
            } else {
                return RedisClient.create(redisURI).connect(redisCodec);
            }
        }, poolConfig == null ? defaultPoolConfig() : poolConfig);
    }

    protected GenericObjectPoolConfig<StatefulConnection<String, ?>> defaultPoolConfig() {
        var poolConfig = new GenericObjectPoolConfig<StatefulConnection<String, ?>>();
        poolConfig.setMaxTotal(20);
        poolConfig.setMaxIdle(8);
        poolConfig.setMinIdle(0);
        return poolConfig;
    }

    /**
     * 从连接池获取一个连接
     * @return redis连接对象
     * @throws Exception
     */

    @SuppressWarnings("unchecked")
    protected <V> StatefulConnection<String, V> connect() throws Exception {
        return (StatefulConnection<String, V>) POOL.borrowObject();
    }

    /**
     * 生成同步命令对象
     * @param conn redis连接
     * @return redis同步命令对象
     */
    private <V> RedisClusterCommands<String, V> sync(StatefulConnection<String, V> conn) {
        if (isCluster()) {
            return ((StatefulRedisClusterConnection<String, V>) conn).sync();
        } else {
            return ((StatefulRedisConnection<String, V>) conn).sync();
        }
    }

    /**
     * 生成异步命令对象
     * @param conn redis连接
     * @return redis异步命令对象
     */
    private <V> RedisClusterAsyncCommands<String, V> async(StatefulConnection<String, V> conn) {
        if (isCluster()) {
            return ((StatefulRedisClusterConnection<String, V>) conn).async();
        } else {
            return ((StatefulRedisConnection<String, V>) conn).async();
        }
    }

    /**
     * 执行同步命令
     *
     * @param function 执行命令方法
     * @return 执行方法返回值
     */
    protected <V, R> R execCmd(Function<RedisClusterCommands<String, V>, R> function) {
        try (StatefulConnection<String, V> connection = connect()) {
            return function.apply(sync(connection));
        } catch (Exception e) {
            log.error("Execute sync command error!", e);
        }
        return null;
    }

    /**
     * 执行异步命令
     *
     * @param function 执行命令方法
     * @return 执行方法返回值
     */
    protected <V, R> R execAsyncCmd(Function<RedisClusterAsyncCommands<String, V>, R> function) {
        try (StatefulConnection<String, V> connection = connect()) {
            return function.apply(async(connection));
        } catch (Exception e) {
            log.error("Execute async command error!", e);
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getExec() throws Exception {
        try (var connection = connect()) {
            return (T) async(connection);
        }
    }
}