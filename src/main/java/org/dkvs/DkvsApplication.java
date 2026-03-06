package org.dkvs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import org.dkvs.config.DkvsProperties;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(DkvsProperties.class)
public class DkvsApplication {

    public static void main(String[] args) {
        SpringApplication.run(DkvsApplication.class, args);
    }
}
