package io.github.natsusai.cache.core.redis.lettuce;

import io.github.natsusai.cache.core.util.KryoUtil;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author liufuhong
 * @since 2020-03-13 10:15
 */

public class KryoCodec implements RedisCodec<String, Object> {

  private Charset charset = StandardCharsets.UTF_8;

  @Override
  public String decodeKey(ByteBuffer bytes) {
    return charset.decode(bytes).toString();
  }

  @Override
  public Object decodeValue(ByteBuffer bytes) {
    byte[] byteArray = new byte[bytes.remaining()];
    bytes.get(byteArray);
    return KryoUtil.readFromByteArray(byteArray);
  }

  @Override
  public ByteBuffer encodeKey(String key) {
    return charset.encode(key);
  }

  @Override
  public ByteBuffer encodeValue(Object value) {
    return ByteBuffer.wrap(KryoUtil.writeToByteArray(value));
  }
}
