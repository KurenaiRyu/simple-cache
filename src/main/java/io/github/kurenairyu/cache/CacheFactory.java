package io.github.kurenairyu.cache;


import io.github.kurenairyu.cache.redis.lettuce.LettuceCache;

/**
 * @author Kurenai
 * @since 2020-03-10 16:02
 */

public class CacheFactory {

    public static Cache create(String host, int port) {
        return new LettuceCache(host, port);
    }

    public static Cache create(String host, int port, String password) {
        return new LettuceCache(host, port, password, 0);
    }

    public static Cache create(String host, int port, CacheType cacheType) {
        switch (cacheType) {
            case REDIS:
            default: return create(host, port);
        }
    }

    public static Cache create(String host, int port, String password, CacheType cacheType) {
        switch (cacheType) {
            case REDIS:
            default: return create(host, port, password);
        }
    }

}
