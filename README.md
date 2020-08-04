# Simple Cache  
![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/NatsuSai/simple-cache?include_prereleases)
![GitHub](https://img.shields.io/github/license/NatsuSai/simple-cache)

项目提供缓存接口用于统一实现，但实现也不一定会完全支持所有接口，当不支持该操作时则默认抛出[NotSupportOperationException](core/src/main/java/io/github/natsusai/cache/core/exception/NotSupportOperationException.java)，
需要注意的是这个异常时runtime异常。

### Features
- 缓存键值带有前缀，前缀采用全大写
- 以命名空间(namespace)和id/键值(key)作为划分应用内部的缓存
- 不调用包含命名空间参数的方法时将以当前对象的类全称作为命名空间
- 默认分隔符用`:`，拼接规则为`prefix:namespace:key`
- 其中只要有一个为空时则会忽略，并不会出现类似`prefix::key`的情况
- 默认不会有超时时间
- 带有raw前缀的方法为不做键值和类名拼接处理（即直接调用底层实现）
- 序列化使用Kryo，以byte数组储存

### How To User
用一下实现的构造方法进行构造一个对象，用Cache接口进行接收，之后调用对象的相应方法进行管理缓存即可  
（暂时只实现了redis）
- Stand-alone(单机)  
JedisCache、LettuceCache
- Cluster(集群)  
ShardedJedisCache、LettuceClusterCache

e.g.
```
Cache cache = new LettuceCache("prefix-string", "localhost", 6379);
User user = cache.get(someKey, User.class);
```

### Install
```shell script
mvn install -DskipTest=true
```

### TODO
- [x] 实现RedisLock  
- [ ] 解耦序列化实现