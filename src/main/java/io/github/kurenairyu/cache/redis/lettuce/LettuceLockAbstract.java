package io.github.kurenairyu.cache.redis.lettuce;


import io.github.kurenairyu.cache.redis.RedisLockAbstract;

/**
 * @author Kurenai
 * @since 2021-07-21 10:32
 */
public abstract class LettuceLockAbstract extends RedisLockAbstract {

    /**
     * 默认构造方法会初始化删除锁的lua脚本，可以自行用set替换
     *
     * @param lockKey                        锁的key
     * @param lockValidityTimeInMilliseconds 锁的有效时间(ms)
     */
    public LettuceLockAbstract(String lockKey, long lockValidityTimeInMilliseconds) {
        super(lockKey, lockValidityTimeInMilliseconds);
    }
}
