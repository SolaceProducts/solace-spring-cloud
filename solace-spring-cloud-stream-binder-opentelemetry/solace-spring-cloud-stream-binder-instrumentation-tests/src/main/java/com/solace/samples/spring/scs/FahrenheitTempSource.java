/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.solace.samples.spring.scs;

import com.solace.samples.spring.common.SensorReading;
import com.solace.samples.spring.common.SensorReading.BaseUnit;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

@SpringBootApplication
public class FahrenheitTempSource {

  private static final Logger log = LoggerFactory.getLogger(FahrenheitTempSource.class);

  private static final UUID sensorIdentifier = UUID.randomUUID();
  private static final Random random = new Random(System.currentTimeMillis());
  private static final int RANDOM_MULTIPLIER = 100;

  public static void main(String[] args) {
    SpringApplication.run(FahrenheitTempSource.class);
  }

  /*
   * Basic Supplier which sends messages every X milliseconds
   * Configurable using spring.cloud.stream.poller.fixed-delay
   */
  //@Bean
  public Supplier<SensorReading> emitSensorReading1() {
    return () -> {
      double temperature = random.nextDouble() * RANDOM_MULTIPLIER;

      SensorReading reading = new SensorReading();
      reading.setSensorID(sensorIdentifier.toString());
      reading.setTemperature(temperature);
      reading.setBaseUnit(BaseUnit.FAHRENHEIT);

      log.info("Emitting " + reading);

      return reading;
    };
  }

  @Bean
  public Supplier<Message<SensorReading>> emitSensorReading() {
    return () -> {
      double temperature = random.nextDouble() * RANDOM_MULTIPLIER;

      SensorReading reading = new SensorReading();
      reading.setSensorID(sensorIdentifier.toString());
      reading.setTemperature(temperature);
      reading.setBaseUnit(BaseUnit.FAHRENHEIT);

      log.info("Emitting " + reading);

      return MessageBuilder.withPayload(reading)
          //.setHeader("solace_correlationId", "accept")
          //.setHeader("solace_correlationId", "reject")
          .setHeader("solace_correlationId", "requeue")
          .build();
    };
  }

  //@Bean
  public Supplier<String> emitHello() {
    return () -> {
      String message = "Hello World!";
      log.info("Emitting: {}", message);
      return message;
    };
  }
}
