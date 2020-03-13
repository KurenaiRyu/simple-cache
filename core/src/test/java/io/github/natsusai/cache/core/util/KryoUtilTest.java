package io.github.natsusai.cache.core.util;

import io.github.natsusai.cache.core.redis.User;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KryoUtilTest {

  @Test
  public void test() {
    User user = new User();
    user.setAge(20);
    user.setName("Kurenai");
    byte[] bytes = KryoUtil.writeToByteArray(user);
    User user1 = KryoUtil.readFromByteArray(bytes);
    assertEquals(user.getName(), user1.getName());
  }

  @Test
  public void testNPE() {
    assertNull(KryoUtil.readFromByteArray(null));
  }
}