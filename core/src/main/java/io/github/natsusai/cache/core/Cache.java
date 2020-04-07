package io.github.natsusai.cache.core;

import io.github.natsusai.cache.core.exception.NotSupportOperationException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * 缓存接口类
 * <p>
 * 除了rawXXX方法外，都将自动构建缓存key
 * </p>
 * <p>
 * 注意：并不是所有实现都支持所提供的方法，当某个实现被调用了不支持的方法时，将会抛出一个Runtime异常
 * </p>
 * @see NotSupportOperationException
 * @author liufuhong
 * @since 2019-08-07 13:20
 */

public interface Cache {

  long VOLATILITY_TIME = 10 * 1000L;//短存时间
  //FIXME：处理缓存短时间内大面积失效的风险

  //----------------------get-------------------------------

  /**
   * 查找缓存
   *
   * @param key 缓存的键值
   * @param <T> 返回类型
   *
   * @return
   */
  <T> T rawGet(String key)
      throws NotSupportOperationException;

  /**
   * 查找缓存，其中命名空间将采用class对象的限定包名（全名）
   *
   * @param <T>   返回类型
   * @param key   缓存标识/id
   * @param clazz 缓存的class对象
   *
   * @return 返回对应缓存对象
   */
  default <T> T get(String key,
                    Class<T> clazz) {

    return get(key, clazz.getName());
  }

  /**
   * 查找缓存，其中命名空间将采用class对象的限定包名（全名），若查找不到则改由回调函数获取并添加进缓存
   *
   * @param <T>      返回类型
   * @param key      缓存标识/id
   * @param clazz    缓存的class对象
   * @param supplier 回调函数
   *
   * @return 返回对应缓存对象
   */
  default <T> T getOrElse(String key,
                          Class<T> clazz,
                          Supplier<T> supplier) {

    return getOrElse(key, clazz.getName(), supplier);
  }

  /**
   * 查找缓存
   *
   * @param key       缓存标识/id
   * @param namespace 命名空间（类似组的概念）
   * @param <T>       返回类型
   *
   * @return 返回对应缓存对象
   */
  <T> T get(String key,
            String namespace);

  /**
   * 查找缓存，若查找不到则改由回调函数获取并添加进缓存（不检测是否存在，直接覆盖）
   *
   * @param key       缓存标识/id
   * @param namespace 命名空间（类似组的概念）
   * @param supplier  回调函数
   * @param <T>       返回类型
   *
   * @return 返回对应缓存对象
   */
  @SuppressWarnings("unchecked")
  default <T> T getOrElse(String key,
                          String namespace,
                          Supplier<T> supplier) {

    return getOrElse(key, namespace, supplier, false);
  }

  /**
   * 查找缓存，若查找不到则改由回调函数获取并添加进缓存
   *
   * @param key       缓存标识/id
   * @param namespace 命名空间（类似组的概念）
   * @param supplier  回调函数
   * @param overWrite 是否覆盖已有缓存
   * @param <T>       返回类型
   *
   * @return 返回对应缓存对象
   */
  @SuppressWarnings("unchecked")
  default <T> T getOrElse(String key,
                          String namespace,
                          Supplier<T> supplier,
                          boolean overWrite) {

    return Optional.ofNullable((T) get(key, namespace))
        .orElseGet(() -> {
          if (!exists(key, namespace)) return null;
          T result = supplier.get();
          if (result == null) {
            set(key, namespace, VOLATILITY_TIME, null);
          } else if (overWrite) {
            set(key, namespace, result);
          } else {
            setIfAbsent(key, namespace, result);
          }
          return result;
        });
  }

  /**
   * 查找缓存，若查找不到则改由回调函数获取并添加进缓存
   *
   * @param key       缓存标识/id
   * @param namespace 命名空间（类似组的概念）
   * @param supplier  回调函数
   * @param ttl       time to live (ms)
   * @param <T>       返回类型
   *
   * @return 返回对应缓存对象
   */
  default <T> T getOrElse(String key,
                          String namespace,
                          long ttl,
                          Supplier<T> supplier) {

    return getOrElse(key, namespace, ttl, supplier, false);
  }

  /**
   * 查找缓存，若查找不到则改由回调函数获取并添加进缓存
   *
   * @param key       缓存标识/id
   * @param namespace 命名空间（类似组的概念）
   * @param supplier  回调函数
   * @param ttl       time to live (ms)
   * @param overWrite 是否覆盖已有缓存
   * @param <T>       返回类型
   *
   * @return 返回对应缓存对象
   */
  @SuppressWarnings("unchecked")
  default <T> T getOrElse(String key,
                          String namespace,
                          long ttl,
                          Supplier<T> supplier,
                          boolean overWrite) {

    return Optional.ofNullable((T) get(key, namespace))
        .orElseGet(() -> {
          if (!exists(key, namespace)) return null;
          T result = supplier.get();
          if (result == null) {
            set(key, namespace, VOLATILITY_TIME, null);
          } else if (overWrite) {
            set(key, namespace, ttl, result);
          } else {
            setIfAbsent(key, namespace, ttl, result);
          }
          return result;
        });
  }

  /**
   * 查找缓存，其中命名空间将采用class对象的限定包名（全名）
   *
   * @param keys  缓存标识/id集合
   * @param clazz 缓存的class对象
   * @param <T>   返回类型
   *
   * @return 返回对应缓存对象
   */
  default <T> List<T> multiGet(Collection<String> keys,
                               Class<T> clazz) {

    return multiGet(keys, clazz.getName());
  }

  /**
   * 查找缓存，其中命名空间将采用class对象的限定包名（全名），若查找不到则改由回调函数获取
   *
   * @param <T>      返回类型
   * @param keys     缓存标识/id集合
   * @param clazz    缓存的class对象
   * @param supplier 回调函数
   *
   * @return 返回对应缓存对象
   */
  default <T> List<T> multiGet(Collection<String> keys,
                               Class<T> clazz,
                               Supplier<List<T>> supplier) {

    return multiGet(keys, clazz.getName(), supplier);
  }

  /**
   * 查找缓存
   *
   * @param keys      缓存标识/id集合
   * @param namespace 命名空间（类似组的概念）
   * @param <T>       返回类型
   *
   * @return 返回对应缓存对象
   */
  <T> List<T> multiGet(Collection<String> keys,
                       String namespace);

  /**
   * 查找缓存，若查找不到则改由回调函数获取
   *
   * @param keys      缓存标识/id集合
   * @param namespace 命名空间（类似组的概念）
   * @param supplier  回调函数
   * @param <T>       返回类型
   *
   * @return 返回对应缓存对象
   */
  @SuppressWarnings("unchecked")
  default <T> List<T> multiGet(Collection<String> keys,
                               String namespace,
                               Supplier<List<T>> supplier) {
    List<T> result = multiGet(keys, namespace);
    if (result == null || result.size() <= 0) return supplier.get();
    return result;
  }

  //----------------------set-------------------------------

  /**
   * 添加缓存，其中命名空间将采用class对象的限定包名（全名）
   *
   * @param key   缓存标识/id
   * @param cache 缓存对象
   * @param <T>   缓存对象类型
   *
   * @return 执行结果
   */
  default <T> void set(String key,
                       T cache) {

    set(key, cache.getClass()
        .getName(), cache);
  }

  /**
   * 添加缓存，键值不做处理
   *
   * @param key   缓存键值
   * @param cache 缓存对象
   * @param <T>   缓存对象类型
   */
  <T> void rawSet(String key,
                  T cache);

  /**
   * 添加缓存，键值不做处理
   *
   * @param key   缓存键值
   * @param ttl   time to live (ms)
   * @param cache 缓存对象
   * @param <T>   缓存对象类型
   */
  <T> void rawSet(String key,
                  long ttl,
                  T cache);

  /**
   * 添加缓存，其中命名空间将采用class对象的限定包名（全名）
   *
   * @param key       缓存标识/id
   * @param namespace 命名空间（类似组的概念）
   * @param cache     缓存对象
   * @param <T>       缓存对象类型
   *
   * @return 执行结果
   */
  <T> void set(String key,
               String namespace,
               T cache);

  /**
   * 添加缓存，其中命名空间将采用class对象的限定包名（全名）
   *
   * @param key   缓存标识/id
   * @param ttl   time to live (ms)
   * @param cache 缓存对象
   * @param <T>   缓存对象类型
   *
   * @return 执行结果
   */
  default <T> void set(String key,
                       long ttl,
                       T cache) {

    set(key, cache.getClass()
        .getName(), ttl, cache);
  }

  /**
   * 添加缓存，其中命名空间将采用class对象的限定包名（全名）
   *
   * @param key       缓存标识/id
   * @param namespace 命名空间（类似组的概念）
   * @param ttl       time to live (ms)
   * @param cache     缓存对象
   * @param <T>       缓存对象类型
   *
   * @return 执行结果
   */
  <T> void set(String key,
               String namespace,
               long ttl,
               T cache);

  /**
   * 添加缓存，其中命名空间将采用class对象的限定包名（全名）
   *
   * @param keyValueMap 缓存key和对象的map
   * @param <T>         缓存对象类型
   *
   * @return 执行结果
   */
  <T> void multiSet(Map<String, T> keyValueMap);

  /**
   * 添加缓存
   *
   * @param namespace   命名空间（类似组的概念）
   * @param keyValueMap 缓存key和对象的map
   * @param <T>         缓存对象类型
   *
   * @return 执行结果
   */
  <T> void multiSet(String namespace,
                    Map<String, T> keyValueMap);

  /**
   * 如果不存在则添加缓存
   *
   * @param key   缓存唯一标识/id
   * @param cache 缓存对象
   * @param <T>   缓存对象类型
   *
   * @return 执行结果
   */
  default <T> Boolean setIfAbsent(String key,
                                  T cache) {

    return setIfAbsent(key, cache.getClass()
        .getName(), cache);
  }

  /**
   * 如果不存在则添加缓存
   *
   * @param key   缓存唯一标识/id
   * @param cache 缓存对象
   * @param <T>   缓存对象类型
   *
   * @return 执行结果
   */
  <T> Boolean rawSetIfAbsent(String key,
                             T cache);

  /**
   * 如果不存在则添加缓存
   *
   * @param key   缓存唯一标识/id
   * @param ttl   time to live (ms)
   * @param cache 缓存对象
   * @param <T>   缓存对象类型
   *
   * @return 执行结果
   */
  <T> Boolean rawSetIfAbsent(String key,
                             long ttl,
                             T cache);

  /**
   * 如果不存在则添加缓存
   *
   * @param key       缓存唯一标识/id
   * @param namespace 命名空间（类似组的概念）
   * @param cache     缓存对象
   * @param <T>       缓存对象类型
   *
   * @return 执行结果
   */
  <T> Boolean setIfAbsent(String key,
                          String namespace,
                          T cache);

  /**
   * 如果不存在则添加缓存
   *
   * @param key       缓存唯一标识/id
   * @param namespace 命名空间（类似组的概念）
   * @param cache     缓存对象
   * @param <T>       缓存对象类型
   *
   * @return 执行结果
   */
  <T> Boolean setIfAbsent(String key,
                          String namespace,
                          long ttl,
                          T cache);

  /**
   * 如果不存在则添加缓存
   *
   * @param keyValueMap 缓存key和对象的map
   * @param <T>         缓存对象类型
   *
   * @return 执行结果
   */
  <T> Boolean multiSetIfAbsent(Map<String, T> keyValueMap);

  /**
   * 如果不存在则添加缓存
   *
   * @param namespace   命名空间（类似组的概念）
   * @param keyValueMap 缓存key和对象的map
   * @param <T>         缓存对象类型
   *
   * @return 执行结果
   */
  <T> Boolean multiSetIfAbsent(String namespace,
                               Map<String, T> keyValueMap);

  /**
   * 如果不存在则添加缓存
   *
   * @param namespace   命名空间（类似组的概念）
   * @param keyValueMap 缓存key和对象的map
   * @param ttl         time to live (ms)
   * @param <T>         缓存对象类型
   *
   * @return 执行结果
   */
  <T> Boolean multiSetIfAbsent(String namespace,
                               long ttl,
                               Map<String, T> keyValueMap);

  //----------------------evict-------------------------------

  /**
   * 移除指定缓存（非模糊匹配）
   *
   * @param key   缓存标识/id
   * @param clazz 缓存的class对象
   *
   * @return 执行结果
   */
  default Boolean remove(String key,
                         Class clazz) {

    return remove(key, clazz.getName());
  }

  /**
   * 移除指定缓存（非模糊匹配）
   *
   * @param key       缓存标识/id
   * @param namespace 命名空间（类似组的概念）
   *
   * @return 执行结果
   */
  Boolean remove(String key,
                 String namespace);

  /**
   * 移除指定缓存（非模糊匹配）
   * </p>
   * 若其中一个列表只有一个，则以1对n进行处理
   *
   * @param keys  缓存标识/id集合
   * @param clazz 缓存的class对象集合
   *
   * @return 执行结果
   */
  default Boolean multiRemoveByClazz(Collection<String> keys,
                                     Class clazz) {

    return multiRemove(keys, clazz.getName());
  }


  /**
   * 移除指定缓存（非模糊匹配）
   * </p>
   * 若其中一个列表只有一个，则以1对n进行处理
   *
   * @param keys      缓存标识/id集合
   * @param namespace 命名空间（类似组的概念）
   *
   * @return 执行结果
   */
  Boolean multiRemove(Collection<String> keys,
                      String namespace);

  /**
   * 移除指定缓存（非模糊匹配）
   *
   * @param key 缓存标识/id集合
   *
   * @return 执行结果
   */
  Boolean rawRemove(String key);

  //----------------------clear-------------------------------

  /**
   * 清除指定class对应的命名空间所有缓存
   *
   * @param clazz java类
   *
   * @return 执行结果
   */
  default <T> Boolean clear(Class<T> clazz) {

    return clear(clazz.getName());
  }

  /**
   * 清除指定命名空间所有缓存
   *
   * @param namespace 命名空间（类似组的概念）
   *
   * @return 执行结果
   */
  Boolean clear(String namespace);

  /**
   * 清除当前应用所有缓存
   *
   * @return 执行结果
   */
  Boolean clear();

  //----------------------other-------------------------------

  /**
   * 查看指定缓存是否存在
   * @param key 键值
   * @param clszz 对象class
   * @return
   */
  default boolean exists(String key, Class clszz) {
    return exists(key, clszz.getName());
  }

  /**
   * 查看指定缓存是否存在
   * @param key 键值
   * @param namespace 命名空间
   * @return
   */
  boolean exists(String key, String namespace);

  /**
   * 查看指定缓存是否存在
   * @param key 键值
   * @return
   */
  boolean rawExists(String key);

  /**
   * 分布式锁
   * <br/>
   * 默认抛出不支持操作异常
   *
   * @param lockKey 锁的key
   * @param lockValidityTimeInMilliseconds 锁的有效时间(ms)
   * @param supplier 被锁的操作
   * @param <R> 被锁方法返回类型
   * @exception NotSupportOperationException 不支持该操作时抛出异常
   * @return 被锁操作返回值
   */
  default <R> R lock(String lockKey, long lockValidityTimeInMilliseconds, Supplier<R> supplier) throws Exception {
    throw new NotSupportOperationException();
  }

  /**
   * 获取调用客户端实例（不进行包装的客户端）
   *
   * @return 客户端实例
   */
  <T> T getClient();
}
