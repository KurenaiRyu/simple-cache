package io.github.kurenairyu.cache;

/**
 * Lock
 *
 * @author Kurenai
 * @since 2020-04-07 14:56
 */

public interface Lock extends AutoCloseable{

  /**
   * 获取锁
   *
   * @return 获取成功返回true
   */
  boolean lock() throws Exception;

  /**
   * 获取锁
   *
   * @param sleepTime 间隔时间(ms)
   * @param times     重试次数
   * @return 获取成功返回true
   */
  boolean lock(long sleepTime, int times) throws Exception;

  /**
   * 尝试获取一次锁
   *
   * @return 获取成功返回true
   */
  boolean tryLockOnce() throws Exception;

  /**
   * 释放锁
   *
   * @return 释放成功返回true, 没有获得锁或者释放失败返回false
   */
  boolean releaseLock() throws Exception;
}
