package io.github.natsusai.cache.core.redis;

import io.github.natsusai.cache.core.Cache;

public interface RedisCache extends Cache {

    boolean isCluster();

}
