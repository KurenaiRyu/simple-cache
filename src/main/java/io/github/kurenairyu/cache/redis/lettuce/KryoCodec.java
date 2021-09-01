package io.github.kurenairyu.cache.redis.lettuce;

import io.github.kurenairyu.cache.util.KryoUtil;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;

/**
 * Kryo Codec
 *
 * @author Kurenai
 * @since 2020-03-13 10:15
 */

public class KryoCodec<V> implements RedisCodec<String, V> {

    private static final byte[] EMPTY = new byte[0];

    @Override
    public String decodeKey(ByteBuffer byteBuffer) {
        return new String(getBytes(byteBuffer));
    }

    @Override
    public V decodeValue(ByteBuffer bytes) {
        return decode(bytes);
    }

    @Override
    public ByteBuffer encodeKey(String s) {
        return ByteBuffer.wrap(s.getBytes());
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

    private static byte[] getBytes(ByteBuffer buffer) {
        int remaining = buffer.remaining();

        if (remaining == 0) {
            return EMPTY;
        }

        byte[] b = new byte[remaining];
        buffer.get(b);
        return b;
    }

}
