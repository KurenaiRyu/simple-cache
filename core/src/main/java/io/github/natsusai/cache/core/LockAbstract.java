package io.github.natsusai.cache.core;

/**
 * @author liufuhong
 * @since 2020-04-07 14:58
 */

public abstract class LockAbstract<K, V> implements Lock {

  protected V uid;
  protected K lockKey;
  protected boolean isGetLock;
  protected long lockValidityTimeInMilliseconds;
  protected String delLockScript;

  /**
   * 默认构造方法会初始化删除锁的lua脚本，可以自行用set替换
   *
   * @param uid                            唯一ID
   * @param lockKey                        锁的key
   * @param lockValidityTimeInMilliseconds 锁的有效时间(ms)
   */
  public LockAbstract(V uid,
                     K lockKey,
                     long lockValidityTimeInMilliseconds) {
    this.uid = uid;
    this.lockKey = lockKey;
    this.lockValidityTimeInMilliseconds = lockValidityTimeInMilliseconds;
    initLuaScript();
  }

  /**
   * 获取锁
   * <p>
   * 重试5次, 每次间隔200ms
   * </p>
   *
   * @return 获取成功返回true
   */
  public boolean lock() {
    return lock(200L, 5);
  }

  /**
   * 获取锁
   *
   * @param sleepTime 间隔时间(ms)
   * @param times     重试次数
   * @return 获取成功返回true
   */
  public boolean lock(long sleepTime, int times) {
    int failCount = 0;
    while (failCount < times) {
      if (tryLockOnce()) return true;
      failCount++;
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return false;
  }

  /**
   * 尝试获取一次锁
   *
   * @return 获取成功返回true
   */
  public abstract boolean tryLockOnce();

  /**
   * 释放锁
   *
   * @return 释放成功返回true, 没有获得锁或者释放失败返回false
   */
  public abstract boolean releaseLock();

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

  public V getUid() {
    return uid;
  }

  public K getLockKey() {
    return lockKey;
  }

  public boolean getIsGetLock() {
    return isGetLock;
  }

  public long getLockValidityTimeInMilliseconds() {
    return lockValidityTimeInMilliseconds;
  }

  public String getDelLockScript() {
    return delLockScript;
  }

  public void setDelLockScript(String delLockScript) {
    this.delLockScript = delLockScript;
  }
}
