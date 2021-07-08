package io.github.natsusai.cache.core.redis.lettuce;

import io.github.natsusai.cache.core.util.KryoUtil;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;

/**
 * Kryo Codec
 *
 * @author Kurenai
 * @since 2020-03-13 10:15
 */

public class KryoCodec<K, V> implements RedisCodec<K, V> {

  @Override
  public K decodeKey(ByteBuffer bytes) {
    return decode(bytes);
  }

  @Override
  public V decodeValue(ByteBuffer bytes) {
    return decode(bytes);
  }

  @Override
  public ByteBuffer encodeKey(K key) {
    return encode(key);
  }

  @Override
  public ByteBuffer encodeValue(V value) {
    return encode(value);
  }

  private <T> T decode(ByteBuffer bytes) {
    return KryoUtil.readFromByteArray(byteBuffer2ByteArray(bytes));
  }

  private <T> ByteBuffer encode(T obj) {
    return ByteBuffer.wrap(KryoUtil.writeToByteArray(obj));
  }

  private byte[] byteBuffer2ByteArray(ByteBuffer byteBuffer) {
    byte[] byteArray = new byte[byteBuffer.remaining()];
    byteBuffer.get(byteArray);
    return byteArray;
  }
}
