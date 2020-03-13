package io.github.natsusai.cache.core.redis.lettuce;

import io.github.natsusai.cache.core.Cache;
import io.github.natsusai.cache.core.redis.User;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LettuceCacheTest {

  private static Cache<RedisCommands<String, Object>> cache;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cache = new LettuceCache("test", "192.168.8.210", 6379, "123456");
    cache.getClient().select(9);
  }

  @Test
  public void testConn() {
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
    User user3 = cache.get(user.getName(), User.class);
    assertNull(user3);
  }
}