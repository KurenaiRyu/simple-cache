package io.github.natsusai.cache.core.redis.lettuce;

import io.github.natsusai.cache.core.Cache;
import io.github.natsusai.cache.core.redis.User;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LettuceCacheTest {

  private static Cache cache;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cache = new LettuceCache("test", "124.160.66.41", 12008);
    RedisCommands<String, Object> client = cache.getClient();
    client.select(9);
  }

  @Test
  public void test() {
    User user = new User();
    user.setAge(20);
    user.setName("Kurenai");
    cache.put(user.getName(), user);
    User user1 = cache.get(user.getName(), User.class);
    assertEquals(user.getName(), user1.getName());
    user.setName("NatsuSai");
    cache.putIfAbsent(user1.getName(), user);
    User user2 = cache.get(user1.getName(), User.class);
    assertEquals(user1.getName(), user2.getName());
    cache.clear(User.class);
    User user3 = cache.get(user.getName(), User.class);
    assertNull(user3);
  }
}