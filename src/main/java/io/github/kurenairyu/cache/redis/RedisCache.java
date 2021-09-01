package io.github.kurenairyu.cache.redis;

import io.github.kurenairyu.cache.Cache;

public interface RedisCache extends Cache {

    boolean isCluster();

}
