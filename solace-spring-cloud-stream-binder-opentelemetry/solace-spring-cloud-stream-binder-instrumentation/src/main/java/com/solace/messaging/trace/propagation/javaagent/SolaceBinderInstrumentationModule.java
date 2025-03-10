/*
 * Copyright 2023-2025 Solace Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.solace.messaging.trace.propagation.javaagent;

import com.google.auto.service.AutoService;
import io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import java.util.Arrays;
import java.util.List;

@AutoService(InstrumentationModule.class)
public final class SolaceBinderInstrumentationModule extends InstrumentationModule {

  public SolaceBinderInstrumentationModule() {
    super("spring-cloud-stream-binder-solace-instrumentation",
        "spring-cloud-stream-binder-solace-instrumentation-1.0.0");
  }

  @Override
  public int order() {
    return 1;
  }

  @Override
  public List<TypeInstrumentation> typeInstrumentations() {
    return Arrays.asList(new SolaceBinderConsumerInstrumentation());
  }

  @Override
  public List<String> getAdditionalHelperClassNames() {
    return Arrays.asList(
        //Solace Binder Consumer Instrumentation Classes
        SolaceBinderConsumerInstrumentation.class.getName(),
        SolaceBinderConsumerInstrumentation.InboundXMLMessageListenerProcessMessageMethodAdvice.class.getName(),
        SolaceBinderConsumerInstrumentation.MessageProducerSupportSendMessageMethodAdvice.class.getName(),

        //Other required classes Solace Binder
        "com.solace.spring.cloud.stream.binder.config.SolaceBinderClientInfoProvider",

        //Other required classes from JCSMP Instrumentation
        "com.solace.messaging.trace.propagation.SolaceJCSMPTextMapGetter",
        "com.solace.messaging.trace.propagation.SolaceJCSMPTextMapSetter",
        "com.solace.messaging.trace.propagation.VersionInfo",
        "com.solace.messaging.trace.propagation.internal.SpanAttributes",
        "com.solace.messaging.trace.propagation.internal.MessagingOperation",
        "com.solace.messaging.trace.propagation.internal.MessagingAttribute",
        "com.solace.messaging.trace.propagation.internal.SpanContextUtil",
        "com.solace.messaging.trace.propagation.internal.TraceStateUtil",
        "com.solace.messaging.trace.propagation.internal.VersionInfoImpl",
        "com.solace.messaging.trace.propagation.internal.ClassLoaderReflectionUtil"
    );
  }
}