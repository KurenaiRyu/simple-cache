package io.github.kurenairyu.cache.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.kurenairyu.cache.redis.User;
import org.junit.Test;

public class JacksonTest {

    ObjectMapper mapper = new ObjectMapper().registerModules(new Jdk8Module(), new JavaTimeModule())
            .activateDefaultTyping(BasicPolymorphicTypeValidator.builder().allowIfBaseType(Object.class).build(), ObjectMapper.DefaultTyping.NON_FINAL);

    @Test
    public void test() throws JsonProcessingException {
        User user = new User();
        user.setAge(20);
        user.setName("Kurenai");
        System.out.println(mapper.writeValueAsString(user));
    }
}