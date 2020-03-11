package com.iotlead.cache.core.redis;

import com.iotlead.cache.core.Cache;
import com.iotlead.cache.core.util.KryoUtil;
import lombok.Data;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RedisCacheTest {

  private static Cache cache;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cache = new RedisCache("192.168.8.210", 6379, "123456", 15);
  }

  @Test
  public void test() {
    User user = new User();
    user.setAge(20);
    user.setName("Kurenai");
    cache.set(user.getName(), user);
    User user1 = cache.get(user.getName(), User.class);
    assertEquals(user.getName(), user1.getName());
    user.setName("NatsuSai");
    cache.setIfAbsent(user1.getName(), user);
    User user2 = cache.get(user1.getName(), User.class);
    assertEquals(user1.getName(), user2.getName());
    cache.clear(User.class);
  }

  @Test
  public void utilTest() {
    User user = new User();
    user.setAge(20);
    user.setName("Kurenai");
    byte[] bytes = KryoUtil.writeToByteArray(user);
    User user1 = KryoUtil.readFromByteArray(bytes);
    assertEquals(user.getName(), user1.getName());
  }

  @Data
  static class User {
    private String name;
    private int age;
  }
}