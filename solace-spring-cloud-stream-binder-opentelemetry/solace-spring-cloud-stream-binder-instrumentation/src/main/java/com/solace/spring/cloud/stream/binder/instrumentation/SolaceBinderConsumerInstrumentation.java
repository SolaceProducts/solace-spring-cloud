package com.solace.spring.cloud.stream.binder.instrumentation;

import static com.solace.messaging.trace.propagation.internal.MessagingAttribute.API_NAME;
import static com.solace.messaging.trace.propagation.internal.MessagingAttribute.API_VERSION;
import static com.solace.messaging.trace.propagation.internal.MessagingAttribute.DELIVERY_MODE;
import static com.solace.messaging.trace.propagation.internal.MessagingAttribute.DESTINATION;
import static com.solace.messaging.trace.propagation.internal.MessagingAttribute.DESTINATION_ANONYMOUS;
import static com.solace.messaging.trace.propagation.internal.MessagingAttribute.DESTINATION_TEMPORARY;
import static com.solace.messaging.trace.propagation.internal.MessagingAttribute.DESTINATION_TYPE;
import static com.solace.messaging.trace.propagation.internal.MessagingAttribute.ERROR_TYPE;
import static com.solace.messaging.trace.propagation.internal.MessagingAttribute.MESSAGE_ID;
import static com.solace.messaging.trace.propagation.internal.MessagingAttribute.OPERATION;
import static com.solace.messaging.trace.propagation.internal.MessagingAttribute.OPERATION_TYPE;
import static com.solace.messaging.trace.propagation.internal.MessagingAttribute.SOLACE_TOPIC_NAME;
import static com.solace.messaging.trace.propagation.internal.MessagingAttribute.SYSTEM;
import static io.opentelemetry.api.trace.SpanKind.CONSUMER;
import static io.opentelemetry.api.trace.SpanKind.INTERNAL;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.namedOneOf;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import com.solace.messaging.trace.propagation.SolaceJCSMPTextMapGetter;
import com.solace.messaging.trace.propagation.SolaceJCSMPTextMapSetter;
import com.solace.spring.cloud.stream.binder.inbound.InboundXMLMessageListener;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.TopicEndpoint;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

public class SolaceBinderConsumerInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<ClassLoader> classLoaderOptimization() {
    return AgentElementMatchers.hasClassesNamed(
        "com.solace.spring.cloud.stream.binder.inbound.InboundXMLMessageListener");
  }

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return AgentElementMatchers.hasSuperType(
        namedOneOf("com.solace.spring.cloud.stream.binder.inbound.InboundXMLMessageListener",
            "org.springframework.integration.endpoint.MessageProducerSupport"));
  }

  @Override
  public void transform(TypeTransformer transformer) {
    //Instrument processMessage method of com.solace.spring.cloud.stream.binder.inbound.InboundXMLMessageListener
    //This "process" span is required for establishing proper link between "receive" (parent) and "process" (child)
    // consumer spans in case of Blocking/Polling receive, and any subsequent spans.
    transformer.applyAdviceToMethod(
        isMethod()
            .and(named("processMessage"))
            .and(takesArguments(1))
            .and(takesArgument(0,
                named("com.solace.spring.cloud.stream.binder.util.MessageContainer"))),
        this.getClass().getName() + "$InboundXMLMessageListenerProcessMessageMethodAdvice"
    );

    //Instrument sendMessage method of org.springframework.integration.endpoint.MessageProducerSupport
    //It's helpful for tracing which SCSt function handled the received message and
    //in case of client side retries it produces a span for each retry attempt.
    //The span kind is set to "internal" and it would be child span of "process" span.
    transformer.applyAdviceToMethod(
        isMethod()
            .and(named("sendMessage"))
            .and(takesArguments(1))
            .and(takesArgument(0,
                named("org.springframework.messaging.Message"))),
        this.getClass().getName() + "$MessageProducerSupportSendMessageMethodAdvice"
    );
  }

  @SuppressWarnings("unused")
  public static class InboundXMLMessageListenerProcessMessageMethodAdvice {

    private InboundXMLMessageListenerProcessMessageMethodAdvice() {
    }

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static Scope before(@Advice.This InboundXMLMessageListener consumer,
        @Advice.Argument(value = 0) MessageContainer messageContainer,
        @Advice.Local("otelSpan") Span span) {

      BytesXMLMessage message = messageContainer.getMessage();
      OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
      Tracer tracer = GlobalOpenTelemetry.getTracer(
          "spring-cloud-stream-binder-solace-instrumentation", "1.0.0");

      Context context = Context.current();
      // If there is no current context associated with the current scope, then the context will be equal to the root context.
      // In this case, extract the context from the message using the TextMapPropagator,
      // use it as the current context and then set this extracted context as the parent of the new span being created.
      if (context == Context.root()) {
        context = openTelemetry.getPropagators().getTextMapPropagator()
            .extract(Context.current(), message, new SolaceJCSMPTextMapGetter());
      }

      String spanDestName;
      String destName;
      String destKind = "";
      String topicName = "";
      boolean isTemporary = false;
      boolean isAnonymous = false;

      Endpoint endpoint = message.getConsumerEndpoint();
      if (endpoint != null) {
        destName = endpoint.getName();
        spanDestName = destName;
        if (endpoint instanceof Queue) {
          destKind = "queue";
          isTemporary = ((Queue) endpoint).isTemporary();
          isAnonymous = isTemporary && ((Queue) endpoint).isAnonymous();
        } else if (endpoint instanceof TopicEndpoint) {
          destKind = "topic endpoint";
          isTemporary = ((TopicEndpoint) endpoint).isTemporary();
          isAnonymous = isTemporary && ((TopicEndpoint) endpoint).isAnonymous();
        }
      } else {
        Topic topic = (Topic) message.getDestination();
        destName = topic.getName();
        spanDestName = "(topic)";
        destKind = "topic";
        isTemporary = topic.isTemporary();
        isAnonymous = isTemporary;
      }

      Span processSpan = tracer.spanBuilder(
              "process " + (isAnonymous ? "(anonymous)" : spanDestName))
          .setSpanKind(CONSUMER)
          .setAttribute(API_NAME.toString(), "spring-cloud-stream-binder-solace")
          //.setAttribute(API_VERSION.toString(), new SolaceBinderClientInfoProvider().getSoftwareVersion())
          .setAttribute(API_VERSION.toString(), "1.0.0")
          .setAttribute(DELIVERY_MODE.toString(), message.getDeliveryMode().toString())
          .setAttribute(DESTINATION.toString(), destName)
          .setAttribute(DESTINATION_TYPE.toString(), destKind)
          .setAttribute(OPERATION.toString(), "consume")
          .setAttribute(OPERATION_TYPE.toString(), "process")
          .setAttribute(SYSTEM.toString(), "SolacePubSub+")
          .setParent(context)
          .startSpan();

      // Add this line to assign the created span to the local variable
      span = processSpan;

      if (destKind.equals("topic endpoint") || destKind.equals("queue")) {
        Destination destination = message.getDestination();
        if (destination instanceof Topic) {
          processSpan.setAttribute(SOLACE_TOPIC_NAME.toString(), destination.getName());
        }
      }

      if (message.getApplicationMessageId() != null) {
        processSpan.setAttribute(MESSAGE_ID.toString(), message.getApplicationMessageId());
      }

      if (isTemporary) {
        processSpan.setAttribute(DESTINATION_TEMPORARY.toString(), true);
      }

      if (isAnonymous) {
        processSpan.setAttribute(DESTINATION_ANONYMOUS.toString(), true);
      }

      Scope scope = processSpan.makeCurrent();
      TextMapPropagator propagator = openTelemetry.getPropagators().getTextMapPropagator();
      propagator.inject(Context.current(), message, new SolaceJCSMPTextMapSetter());

      return scope;
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
    public static void onExit(@Advice.Thrown Throwable throwable,
        @Advice.Local("otelSpan") Span span,
        @Advice.Enter Scope scope) {

      //If an exception was thrown during the method's execution, do the following
      if (throwable != null) {
        span.setAttribute(ERROR_TYPE.toString(), throwable.getClass().getName());//Set error.type
        span.recordException(throwable); // Record the exception
        span.setStatus(StatusCode.ERROR); // Set the span status to ERROR
      }

      //Close the scope.
      scope.close();

      //End the span. This makes it ready to be exported to the configured exporter (e.g., Jaeger, Zipkin, OTLP).
      span.end();
    }
  }

  @SuppressWarnings("unused")
  public static class MessageProducerSupportSendMessageMethodAdvice {

    private MessageProducerSupportSendMessageMethodAdvice() {
    }

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static Scope before(@Advice.This MessageProducerSupport consumer,
        @Advice.Argument(value = 0) Message<?> message,
        @Advice.Local("otelSpan") Span span) {

      OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
      Tracer tracer = GlobalOpenTelemetry.getTracer(
          "spring-cloud-stream-binder-solace-instrumentation",
          "1.0.0");

      Context context = Context.current();

      MessageChannel channel = consumer.getOutputChannel();
      String channelName = "";
      if (channel instanceof AbstractMessageChannel) {
        channelName = ((AbstractMessageChannel) channel).getFullChannelName();
      } else if (channel instanceof org.springframework.messaging.support.AbstractMessageChannel) {
        channelName =
            ((org.springframework.messaging.support.AbstractMessageChannel) channel).getBeanName();
      } else if (channel != null) {
        channelName = channel.getClass().getSimpleName();
      }

      Span internalSpan = tracer.spanBuilder(channelName + " process")
          .setSpanKind(INTERNAL)
          .setAttribute(API_NAME.toString(), "spring-cloud-stream-binder-solace")
          .setAttribute(API_VERSION.toString(), "1.0.0")
          .setAttribute(OPERATION.toString(), "consume")
          .setAttribute(OPERATION_TYPE.toString(), "process")
          .setAttribute(SYSTEM.toString(), "SolacePubSub+")
          .setParent(context)
          .startSpan();

      // Add this line to assign the created span to the local variable
      span = internalSpan;

      Scope scope = internalSpan.makeCurrent();
      TextMapPropagator propagator = openTelemetry.getPropagators().getTextMapPropagator();
      propagator.inject(Context.current(), message, new SolaceJCSMPTextMapSetter());

      return scope;
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
    public static void onExit(@Advice.Thrown Throwable throwable,
        @Advice.Local("otelSpan") Span span,
        @Advice.Enter Scope scope) {

      //If an exception was thrown during the method's execution, do the following
      if (throwable != null) {
        span.setAttribute(ERROR_TYPE.toString(), throwable.getClass().getName()); //Set  error.type
        span.recordException(throwable); //Record the exception
        span.setStatus(StatusCode.ERROR);//Set the span status to ERROR
      }

      //Close the scope to end it.
      scope.close();

      //End the span. This makes it ready to be exported to the configured exporter (e.g., Jaeger, Zipkin, OTLP).
      span.end();
    }
  }
}