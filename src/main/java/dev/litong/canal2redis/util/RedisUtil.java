package dev.litong.canal2redis.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.concurrent.TimeUnit;

/**
 * Redis工具类
 *
 * @author litong
 */
@Component
public class RedisUtil {

    /**
     * 默认过期时长（秒）
     */
    private final static long DEFAULT_EXPIRE = 1800;

    private final RedisTemplate<String, Object> redisTemplate;

    @Autowired
    public RedisUtil(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 设置过期时长
     *
     * @param key     键
     * @param seconds 过期时长（秒）
     */
    public void expire(String key, long seconds) {
        if (seconds > 0) {
            redisTemplate.expire(key, seconds, TimeUnit.SECONDS);
        }
    }

    /**
     * 设置默认的过期时长
     *
     * @param key 键
     */
    public void expireDefault(String key) {
        redisTemplate.expire(key, DEFAULT_EXPIRE, TimeUnit.SECONDS);
    }

    /**
     * 获取过期时长（秒）
     *
     * @param key 键
     * @return 时长（秒）
     */
    public long getExpire(String key) {
        return redisTemplate.getExpire(key, TimeUnit.SECONDS);
    }

    /**
     * 是否包含某键
     *
     * @param key 键
     * @return true 包含 false 不包含
     */
    public boolean hasKey(String key) {
        return redisTemplate.hasKey(key);
    }

    /**
     * 删除某键
     *
     * @param key 键
     */
    public void delete(String... key) {
        if (key != null && key.length > 0) {
            if (key.length == 1) {
                redisTemplate.delete(key[0]);
            } else {
                redisTemplate.delete(CollectionUtils.arrayToList(key));
            }
        }
    }

    /**
     * 获取key的value
     *
     * @param key 键
     * @return 值
     */
    public Object get(String key) {
        return key == null ? null : redisTemplate.opsForValue().get(key);
    }

    /**
     * 永久set
     *
     * @param key   键
     * @param value 值
     */
    public void set(String key, Object value) {
        redisTemplate.opsForValue().set(key, value);
    }

    /**
     * 默认set 默认实效时间 DEFAULT_EXPIRE
     *
     * @param key   键
     * @param value 值
     */
    public void setDefault(String key, Object value) {
        this.set(key, value, DEFAULT_EXPIRE);
    }

    /**
     * 普通缓存放入并设置时间
     *
     * @param key     键
     * @param value   值
     * @param seconds 时间(秒)
     */
    public void set(String key, Object value, long seconds) {
        if (seconds > 0) {
            redisTemplate.opsForValue().set(key, value, seconds, TimeUnit.SECONDS);
        } else {
            set(key, value);
        }
    }

    /**
     * 递增
     *
     * @param key   键
     * @param delta 步长
     * @return 递增之后的结果
     */
    public long incr(String key, long delta) {
        if (delta <= 0) {
            throw new RuntimeException("步长必须大于0");
        }
        return redisTemplate.opsForValue().increment(key, delta);
    }

    /**
     * 递减
     *
     * @param key   键
     * @param delta 步长
     * @return 递减之后的结果
     */
    public long decr(String key, long delta) {
        if (delta <= 0) {
            throw new RuntimeException("步长必须大于0");
        }
        return redisTemplate.opsForValue().increment(key, -delta);
    }


}
