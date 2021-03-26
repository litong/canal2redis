package dev.litong.canal2redis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * @author litong
 */
@EnableScheduling
@SpringBootApplication
@EnableTransactionManagement
public class Canal2RedisApplication {

    public static void main(String[] args) {
        SpringApplication.run(Canal2RedisApplication.class, args);
    }

}

