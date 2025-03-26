package com.solace.spring.cloud.stream.binder.instrumentation;

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