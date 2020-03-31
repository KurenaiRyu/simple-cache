package io.github.natsusai.cache.micronaut.starter;

import io.github.natsusai.cache.core.Cache;
import io.lettuce.core.RedisClient;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;

/**
 * @author liufuhong
 * @since 2020-03-17 14:52
 */

public class App {
  public static void main(String[] args) {
    ApplicationContext context = Micronaut.run(App.class, args);
    RedisClient redisClient = context.getBean(RedisClient.class);
    System.out.println("redisClient = " + redisClient);
    RedisCacheFactory redisCacheFactory = context.getBean(RedisCacheFactory.class);
    System.out.println("redisCacheFactory = " + redisCacheFactory);
    Cache cache = context.getBean(Cache.class);
    System.out.println("cache = " + cache);
    User user = new User();
    user.name = "testName";
    user.age = 16;
    cache.set("test", user);
    User user1 = cache.get("test", User.class);
    System.out.println("user1.getName().equals(user.getName()) = " + user1.getName().equals(user.getName()));
    cache.remove("test", User.class);
  }

  static class User {
    private String name;
    private int age;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }
  }
}
