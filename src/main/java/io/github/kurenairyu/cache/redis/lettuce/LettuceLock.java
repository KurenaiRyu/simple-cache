package io.github.kurenairyu.cache.redis.lettuce;

import io.github.kurenairyu.cache.util.SnowFlakeGenerator;
import io.lettuce.core.api.sync.RedisTransactionalCommands;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class LettuceLock extends LettuceLockAbstract {

    private final LettuceCacheAbstract cache;
    private final SnowFlakeGenerator   snowFlakeGenerator = SnowFlakeGenerator.getInstance();

    public LettuceLock(String lockKey, long lockValidityTimeInMilliseconds, LettuceCacheAbstract cache) {
        super(lockKey, lockValidityTimeInMilliseconds);
        this.cache = cache;
    }

    /**
     * 尝试获取一次锁
     *
     * @return 获取成功返回true
     */
    public synchronized boolean tryLockOnce() throws Exception {
        locked.set(false);
        uid = snowFlakeGenerator.nextId();
        return cache.execCmd(cmd -> {
            ((RedisTransactionalCommands<?, ?>) cmd).multi();
            var absent = cmd.setnx(lockKey, uid);
            locked.set(absent != null && absent);
            if (locked.get()) {
                cmd.pexpire(lockKey, ttl);
                ((RedisTransactionalCommands<?, ?>) cmd).exec();
            } else {
                ((RedisTransactionalCommands<?, ?>) cmd).discard();
                log.debug("锁[{}]争夺失败， 当前锁uid: {}", lockKey, cmd.get(lockKey));
            }
            return locked.get();
        });
    }

    /**
     * 释放锁
     *
     * @return 释放成功返回true, 没有获得锁或者释放失败返回false
     */
    public boolean releaseLock() {
        return cache.execCmd(cmd -> {
            if (!locked.get()) return true;
            ((RedisTransactionalCommands<?, ?>) cmd).multi();
            var currentUid = cmd.get(lockKey);
            if (currentUid == null) {
                log.debug("当前锁[{}]为空，非正常释放! 本次uid: {}", lockKey, uid);
                return false;
            } else if (currentUid.equals(uid)) {
                if (cmd.del(lockKey) > 0) {
                    log.debug("释放锁[{}]，本次uid: {}", lockKey, uid);
                } else {
                    log.debug("释放锁[{}]失败，本次uid: {}", lockKey, uid);
                    return false;
                }
            } else {
                log.debug("锁[{}]非正常释放，本次uid: {}，当前锁uid: {}", lockKey, uid, currentUid);
            }
            ((RedisTransactionalCommands<?, ?>) cmd).exec();
            return true;
        });
    }

    @Override
    public void close() throws Exception {
        releaseLock();
    }
}