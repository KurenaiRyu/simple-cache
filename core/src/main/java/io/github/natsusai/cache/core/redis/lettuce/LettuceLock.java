package io.github.natsusai.cache.core.redis.lettuce;

import io.github.natsusai.cache.core.LockAbstract;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LettuceLock<K, V> extends LockAbstract<K, V> {

  private final Logger logger = LoggerFactory.getLogger(LettuceLock.class);
  private final RedisCommands<K, V> commands;

  /**
   * 默认构造方法会初始化删除锁的lua脚本，可以自行用set替换
   *
   * @param uid                            唯一ID
   * @param lockKey                        锁的key
   * @param lockValidityTimeInMilliseconds 锁的有效时间(ms)
   * @param commands                       Redis Commands
   */
  public LettuceLock(V uid,
                     K lockKey,
                     long lockValidityTimeInMilliseconds,
                     RedisCommands<K, V> commands) {
    super(uid, lockKey, lockValidityTimeInMilliseconds);
    this.commands = commands;
  }

  /**
   * 尝试获取一次锁
   *
   * @return 获取成功返回true
   */
  public boolean tryLockOnce() {
    Boolean absent = commands.setnx(lockKey, uid);
    isGetLock = absent != null && absent;
    if (isGetLock) {
      commands.pexpire(lockKey, lockValidityTimeInMilliseconds);
    } else {
      logger.debug("锁[{}]争夺失败， 当前锁uid: {}", lockKey, commands.get(lockKey));
    }
    return isGetLock;
  }

  /**
   * 释放锁
   *
   * @return 释放成功返回true, 没有获得锁或者释放失败返回false
   */
  @SuppressWarnings("unchecked")
  public boolean releaseLock() {
    if (!isGetLock) return false;

    Integer result;
    K[] keys = (K[]) new Object[]{lockKey};
    try {
      result = commands.eval(delLockScript, ScriptOutputType.INTEGER, keys, uid);
    } catch (NullPointerException ex) {
      logger.debug("当前锁[{}]为空，非正常释放! 本服务uid: {}", lockKey, uid);
      return false;
    }
    if (result == null || result == 0) {
      logger.debug("锁[{}]释放失败! 本服务uid: {}, 当前锁uid: {}", lockKey, uid, commands.get(lockKey));
      return false;
    }
    return true;
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

  @Override
  public void close() throws Exception {
    releaseLock();
  }
}