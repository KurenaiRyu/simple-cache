# Simple Cache  
![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/KurenaiRyu/simple-cache?include_prereleases)
![GitHub](https://img.shields.io/github/license/KurenaiRyu/simple-cache)

项目提供缓存接口用于统一实现，但实现也不一定会完全支持所有接口，当不支持该操作时则默认抛出[NotSupportOperationException](core/src/main/java/io/github/natsusai/cache/core/exception/NotSupportOperationException.java)，
需要注意的是这个异常时runtime异常。

## Features
- 以命名空间(namespace)和id/主键(key)作为划分
- 默认分隔符用`:`，拼接规则为`namespace:key`
- 默认不会有超时时间
- 序列化使用Kryo，以byte数组储存
- 已加入连接池

### How To User
用一下实现的构造方法进行构造一个对象，用`Cache`或`RedisCache`是接口进行接收，之后调用对象的相应方法进行管理缓存即可  
（暂时只实现了redis其中一种）  
- LettuceCache
  
    ```java
    e.g.
    Cache cache = CacheFactory("host", 6379);
    or
    Cache cache = new LettuceCache("localhost", 6379);
  
    User user = cache.get(User.class.getName(), 123L);
    ```

## Install
```shell script
mvn install -DskipTest=true
```

## TODO
- [ ] 实现RedisLock  
- [x] 解耦序列化实现

## Thanks

> [IntelliJ IDEA](https://zh.wikipedia.org/zh-hans/IntelliJ_IDEA) 是一个在各个方面都最大程度地提高开发人员的生产力的 IDE，适用于 JVM 平台语言。

特别感谢 [JetBrains](https://www.jetbrains.com/?from=simple-cache) 为开源项目提供免费的 [IntelliJ IDEA](https://www.jetbrains.com/idea/?from=simple-cache) 等 IDE 的授权  
[<img src=".github/jetbrains.png" width="200"/>](https://www.jetbrains.com/?from=simple-cache)
