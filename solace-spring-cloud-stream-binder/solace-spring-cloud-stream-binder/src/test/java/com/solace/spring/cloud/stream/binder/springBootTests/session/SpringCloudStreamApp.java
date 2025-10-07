package com.solace.spring.cloud.stream.binder.springBootTests.session;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

@SpringBootApplication
public class SpringCloudStreamApp {

  public static void main(String[] args) {
    SpringApplication.run(SpringCloudStreamApp.class, args);
  }

  @Bean
  public Consumer<Message<?>> sink() {
    return (msg -> System.out.println(msg.getPayload()));
  }

  @Bean
  public Function<String, String> processor() {
    return (msg) -> msg;
  }

  @Bean
  public Supplier<String> source() {
    return () -> "test";
  }

  @Bean
  public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
    http.authorizeHttpRequests(requests -> requests
        .requestMatchers(new AntPathRequestMatcher("/actuator/*")).permitAll()
        .anyRequest().authenticated());

    return http.build();
  }
}