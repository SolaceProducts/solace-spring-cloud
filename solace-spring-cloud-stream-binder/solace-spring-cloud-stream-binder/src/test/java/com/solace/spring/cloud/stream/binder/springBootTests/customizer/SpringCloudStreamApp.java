package com.solace.spring.cloud.stream.binder.springBootTests.customizer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringCloudStreamApp {

    public static void main(String[] args) {
        SpringApplication.run(SpringCloudStreamApp.class, args);
    }
}
