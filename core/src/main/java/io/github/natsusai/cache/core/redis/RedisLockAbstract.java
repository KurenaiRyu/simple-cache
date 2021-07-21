package io.github.natsusai.cache.core.redis;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Kurenai
 * @since 2020-04-07 14:58
 */

public abstract class RedisLockAbstract implements RedisLock {

    protected long          uid;
    protected String        lockKey;
    protected AtomicBoolean locked;
    protected long          ttl;
    protected String  delLockScript;

    /**
     * 默认构造方法会初始化删除锁的lua脚本，可以自行用set替换
     *
     * @param lockKey                        锁的key
     * @param ttl 锁的有效时间(ms)
     */
    public RedisLockAbstract(String lockKey,
                             long ttl) {
        this.lockKey = lockKey;
        this.ttl = ttl;
        initLuaScript();
    }

    /**
     * 获取锁
     *
     * @return 获取成功返回true
     */
    public synchronized boolean lock() throws Exception {
        return lock(200L, 3);
    }

    /**
     * 获取锁
     *
     * @param sleepTime 间隔时间(ms)
     * @param times     重试次数
     * @return 获取成功返回true
     */
    public synchronized boolean lock(long sleepTime, int times) throws Exception {
        int failCount = 0;
        while (failCount < times) {
            if (tryLockOnce()) return true;
            failCount++;
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException ignored) {

            }
        }
        return false;
    }

    /**
     * 尝试获取一次锁
     *
     * @return 获取成功返回true
     */
    public abstract boolean tryLockOnce() throws Exception;

    /**
     * 释放锁
     *
     * @return 释放成功返回true, 没有获得锁或者释放失败返回false
     */
    public abstract boolean releaseLock() throws Exception;

    @Override
    public void close() throws Exception {
        releaseLock();
    }

    /**
     * 初始化lua脚本
     */
    private void initLuaScript() {
        //从redis获取该key的值，若与传入参数相同则删除该值
        delLockScript = "if redis.call('get',KEYS[1]) == ARGV[1] then " +
                "  return redis.call('del',KEYS[1]) " +
                "else " +
                "  return 0 " +
                "end";
    }

    public void setDelLockScript(String delLockScript) {
        this.delLockScript = delLockScript;
    }
}
