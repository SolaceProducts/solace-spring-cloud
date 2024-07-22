package com.solace.spring.cloud.stream.binder.springBootTests.oauth2;

import java.util.function.Consumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

@SpringBootApplication
@EnableWebSecurity
public class SpringCloudStreamOAuth2App {

  public static void main(String[] args) {
    SpringApplication.run(SpringCloudStreamOAuth2App.class, args);
  }

  @Bean
  public Consumer<Message<?>> consume() {
    return (msg -> System.out.println(msg.getPayload()));
  }

  @Bean
  public Consumer<Message<?>> otherConsume() {
    return (msg -> System.out.println(msg.getPayload()));
  }

  @Bean
  public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
    http.authorizeHttpRequests(requests -> requests
        .requestMatchers(new AntPathRequestMatcher("/actuator/*")).permitAll()
        .anyRequest().authenticated());

    return http.build();
  }
}