package io.github.kurenairyu.cache.redis.lettuce.jackson;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.lettuce.core.codec.RedisCodec;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.core.util.Assert;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Kryo Codec
 *
 * @author Kurenai
 * @since 2020-03-13 10:15
 */

@Log4j2
public class JacksonCodec<V> implements RedisCodec<String, V> {

    private static final byte[] EMPTY = new byte[0];

    private final ObjectMapper mapper;

    public JacksonCodec() {
        this.mapper = defaultMapper();
    }

    public JacksonCodec(ObjectMapper mapper) {
        Assert.requireNonEmpty(mapper, "ObjectMapper can not be empty");
        this.mapper = mapper;
    }

    @Override
    public String decodeKey(ByteBuffer byteBuffer) {
        return new String(getBytes(byteBuffer));
    }

    @Override
    @SuppressWarnings("unchecked")
    public V decodeValue(ByteBuffer byteBuffer) {
        try {
            return (V) mapper.readValue(getBytes(byteBuffer), Object.class);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public ByteBuffer encodeKey(String s) {
        return ByteBuffer.wrap(s.getBytes());
    }

    @Override
    public ByteBuffer encodeValue(V value) {
        try {
            return ByteBuffer.wrap(mapper.writeValueAsBytes(value));
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    private ObjectMapper defaultMapper() {
        return new ObjectMapper().registerModules(new Jdk8Module(), new JavaTimeModule(), new RecordNamingStrategyPatchModule())
                .enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING)
                .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
                .activateDefaultTyping(BasicPolymorphicTypeValidator.builder().allowIfBaseType(Object.class).build(), ObjectMapper.DefaultTyping.EVERYTHING);
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
