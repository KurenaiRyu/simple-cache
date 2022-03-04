package io.github.kurenairyu.cache.redis.lettuce;

import io.github.kurenairyu.cache.redis.RedisCacheAbstract;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static io.github.kurenairyu.cache.util.StringPool.COLON;

/**
 * Lettuce Cache Abstract
 *
 * @author Kurenai
 * @since 2020-03-10 15:56
 */
@Log4j2
public abstract class LettuceCacheAbstract extends RedisCacheAbstract {

    protected final GenericObjectPool<StatefulRedisConnection<String, ?>> POOL;
    protected final String                                                CONNECTOR = COLON;

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

    public LettuceCacheAbstract(RedisURI redisURI, RedisCodec<String, ?> redisCodec, GenericObjectPoolConfig<StatefulRedisConnection<String, ?>> poolConfig) {
        this.POOL = ConnectionPoolSupport.createGenericObjectPool(() -> RedisClient.create(redisURI).connect(redisCodec), poolConfig == null ? defaultPoolConfig() : poolConfig);
    }

    protected GenericObjectPoolConfig<StatefulRedisConnection<String, ?>> defaultPoolConfig() {
        var poolConfig = new GenericObjectPoolConfig<StatefulRedisConnection<String, ?>>();
        poolConfig.setMaxTotal(20);
        poolConfig.setMaxIdle(8);
        poolConfig.setMinIdle(0);
        return poolConfig;
    }

    /**
     * 从连接池获取一个连接
     *
     * @return redis连接对象
     * @throws Exception
     */

    @SuppressWarnings("unchecked")
    protected <V> StatefulRedisConnection<String, V> connect() {
        try {
            return (StatefulRedisConnection<String, V>) POOL.borrowObject();
        } catch (Exception e) {
            throw new RuntimeException("Borrow connection fail", e);
        }
    }

    /**
     * 执行同步命令
     *
     * @param function 执行命令方法
     * @return 执行方法返回值
     */
    protected <V, R> R execCmd(Function<RedisCommands<String, V>, R> function) {
        StatefulRedisConnection<String, V> connection = connect();
        try {
            return function.apply(connection.sync());
        } catch (Exception e) {
            log.error("Execute sync command error!", e);
            throw new RuntimeException("Execute sync command error!", e);
        } finally {
            if (connection != null) POOL.returnObject(connection);
        }
    }

    /**
     * 执行异步命令
     *
     * @param function 执行命令方法
     * @return 执行方法返回值
     */
    @SuppressWarnings("unchecked")
    protected <V, R> CompletableFuture<R> execAsyncCmd(Function<RedisAsyncCommands<String, V>, CompletionStage<R>> function) {
        StatefulRedisConnection<String, V> connection = connect();
        return (CompletableFuture<R>) function.apply(connection.async()).handle((r, e) -> {
            POOL.returnObject(connection);
            if (e == null) {
                return CompletableFuture.completedFuture(r);
            } else {
                log.error("Execute sync command error!", e);
                return CompletableFuture.failedFuture(e);
            }
        }).thenCompose(f -> f);
    }

    public void shutdown() {
        POOL.close();
    }
}
