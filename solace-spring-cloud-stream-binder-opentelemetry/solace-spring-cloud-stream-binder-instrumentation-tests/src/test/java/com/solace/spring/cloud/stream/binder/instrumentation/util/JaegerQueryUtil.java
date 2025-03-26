package com.solace.spring.cloud.stream.binder.instrumentation.util;

import static junit.framework.TestCase.fail;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.solace.spring.cloud.stream.binder.config.SolaceBinderClientInfoProvider;
import com.solacesystems.jcsmp.JCSMPVersion;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.jaegertracing.api_v3.QueryServiceGrpc;
import io.jaegertracing.api_v3.QueryServiceGrpc.QueryServiceBlockingStub;
import io.jaegertracing.api_v3.QueryServiceOuterClass.FindTracesRequest;
import io.jaegertracing.api_v3.QueryServiceOuterClass.TraceQueryParameters;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Span.SpanKind;
import io.opentelemetry.proto.trace.v1.TracesData;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testcontainers.shaded.org.awaitility.Awaitility;

public class JaegerQueryUtil {

  public static final String OPERATION_RECEIVE = "receive";
  public static final String OPERATION_PROCESS = "process";
  public static final String OPERATION_CONSUME = "consume";
  public static final String OPERATION_PUBLISH = "publish";
  public static final String OPERATION_SEND = "send";

  public static final String TAG_MSG_OPERATION = "messaging.operation";
  public static final String TAG_MESSAGING_SYSTEM = "messaging.system";
  public static final String TAG_MSG_DEST = "messaging.destination.name";
  public static final String TAG_MSG_OPERATION_NAME = "messaging.operation.name";
  public static final String TAG_MSG_OPERATION_TYPE = "messaging.operation.type";
  public static final String TAG_MSG_SOLACE_API_NAME = "messaging.solace.api.name";
  public static final String TAG_MSG_SOLACE_API_VERSION = "messaging.solace.api.version";
  public static final String SOLACE_API_NAME_SCST_BINDER = "spring-cloud-stream-binder-solace";
  public static final String TAG_SOLACE_DESTINATION_TYPE = "messaging.solace.destination.type";
  public static final String TAG_MSG_SOLACE_TOPIC = "messaging.solace.message.topic";

  public static final String MESSAGING_SYSTEM = "SolacePubSub+";
  public static final String SOLACE_API_NAME_JCSMP = "jcsmp";
  public static final String SOLACE_API_JCSMP_VERSION = new JCSMPVersion().getSwVersion();
  public static final String SOLACE_API_NAME_SCST_BINDER_VERSION = SolaceBinderClientInfoProvider.VERSION;

  public static KeyValue createTag(String key, String value) {
    return KeyValue.newBuilder().setKey(key)
        .setValue(AnyValue.newBuilder().setStringValue(value).build()).build();
  }

  private static List<KeyValue> createBasicTags(String operation, String operationType,
      String apiName, String apiVersion) {
    List<KeyValue> tags = new ArrayList<>();
    tags.add(createTag(TAG_MSG_OPERATION_NAME, operation));
    tags.add(createTag(TAG_MSG_OPERATION_TYPE, operationType));
    tags.add(createTag(TAG_MSG_SOLACE_API_NAME, apiName));
    tags.add(createTag(TAG_MSG_SOLACE_API_VERSION, apiVersion));
    tags.add(createTag(TAG_MESSAGING_SYSTEM, MESSAGING_SYSTEM));
    return tags;
  }

  public static List<ByteString> verifyPublishSpans(List<TracesData> traces, int expectedSpans,
      String destName, String destType) {
    List<KeyValue> tags = createBasicTags(OPERATION_PUBLISH, OPERATION_PUBLISH,
        SOLACE_API_NAME_JCSMP, SOLACE_API_JCSMP_VERSION);
    tags.add(createTag(TAG_MSG_DEST, destName));
    tags.add(createTag(TAG_SOLACE_DESTINATION_TYPE, destType));

    return verifySpans(traces, SpanKind.SPAN_KIND_PRODUCER, tags, expectedSpans);
  }

  private static boolean isQueueOrTopicEndpoint(String destType) {
    return "queue".equals(destType) || "topic endpoint".equals(destType);
  }

  public static void verifyConsumerReceiveSpans(List<TracesData> traces, int expectedSpans,
      String destName, String destType, String topicSub) {
    List<KeyValue> tags = createBasicTags(OPERATION_CONSUME, OPERATION_RECEIVE,
        SOLACE_API_NAME_JCSMP, SOLACE_API_JCSMP_VERSION);
    tags.add(createTag(TAG_MSG_DEST, destName));
    tags.add(createTag(TAG_SOLACE_DESTINATION_TYPE, destType));

    if (isQueueOrTopicEndpoint(destType) && topicSub != null) {
      tags.add(createTag(TAG_MSG_SOLACE_TOPIC, topicSub));
    }

    verifySpans(traces, SpanKind.SPAN_KIND_CONSUMER, tags, expectedSpans);
  }

  public static void verifyConsumerProcessSpans(List<TracesData> traces, int expectedSpans,
      String destName, String destType, String topicSub) {
    List<KeyValue> tags = createBasicTags(OPERATION_CONSUME, OPERATION_PROCESS,
        SOLACE_API_NAME_SCST_BINDER, SOLACE_API_NAME_SCST_BINDER_VERSION);
    tags.add(createTag(TAG_MSG_DEST, destName));
    tags.add(createTag(TAG_SOLACE_DESTINATION_TYPE, destType));

    if (isQueueOrTopicEndpoint(destType) && topicSub != null) {
      tags.add(createTag(TAG_MSG_SOLACE_TOPIC, topicSub));
    }

    verifySpans(traces, SpanKind.SPAN_KIND_CONSUMER, tags, expectedSpans);
  }

  public static void verifyConsumerInternalSpans(List<TracesData> traces, int expectedSpans) {
    List<KeyValue> tags = createBasicTags(OPERATION_CONSUME, OPERATION_PROCESS,
        SOLACE_API_NAME_SCST_BINDER, SOLACE_API_NAME_SCST_BINDER_VERSION);
    verifySpans(traces, SpanKind.SPAN_KIND_INTERNAL, tags, expectedSpans);
  }

  private static void verifyBrokerOperationSpans(List<TracesData> traces, int expectedSpans,
      SpanKind spanKind, String operation, KeyValue... additionalTags) {
    List<KeyValue> tags = new ArrayList<>();
    tags.add(createTag(TAG_MSG_OPERATION, operation));
    tags.add(createTag(TAG_MESSAGING_SYSTEM, MESSAGING_SYSTEM));

    if (additionalTags != null) {
      tags.addAll(List.of(additionalTags));
    }

    verifySpans(traces, spanKind, tags, expectedSpans);
  }

  public static void verifyBrokerReceiveSpans(List<TracesData> traces, int expectedSpans,
      KeyValue... additionalTags) {
    verifyBrokerOperationSpans(traces, expectedSpans, SpanKind.SPAN_KIND_CONSUMER,
        OPERATION_RECEIVE, additionalTags);
  }

  public static void verifyBrokerSendSpans(List<TracesData> traces, int expectedSpans,
      KeyValue... additionalTags) {
    verifyBrokerOperationSpans(traces, expectedSpans, SpanKind.SPAN_KIND_PRODUCER, OPERATION_SEND,
        additionalTags);
  }

  public static List<TracesData> findTraces(String jaegerQueryServerUrl, String serviceName,
      int expectedTraces, int expectedSpans) {
    ManagedChannel channel = ManagedChannelBuilder.forTarget(jaegerQueryServerUrl).usePlaintext()
        .build();

    try {
      QueryServiceBlockingStub querySrvStub = QueryServiceGrpc.newBlockingStub(channel);
      List<TracesData> traces = new LinkedList<>();
      Awaitility.await().pollInterval(Duration.ofSeconds(3)).atMost(20, TimeUnit.SECONDS)
          .until(() -> {
            try {
              traces.clear();
              traces.addAll(findTracesWithinLastFiveMinutes(querySrvStub, serviceName));
              return traces.size() >= expectedTraces
                  && countSpans(traces.get(traces.size() - 1)) >= expectedSpans;
            } catch (Exception e) {
              return false;
            }
          });
      return traces;
    } finally {
      channel.shutdown();
    }
  }

  public static List<TracesData> findTracesWithinLastFiveMinutes(
      QueryServiceBlockingStub queryService, String service) {
    long currentTime = System.currentTimeMillis();
    Timestamp startTimeMin = Timestamp.newBuilder()
        .setSeconds((currentTime - (5 * 60 * 1000)) / 1000).build();
    Timestamp startTimeMax = Timestamp.newBuilder().setSeconds(currentTime / 1000).build();

    TraceQueryParameters queryParameters = TraceQueryParameters.newBuilder().setServiceName(service)
        .setStartTimeMin(startTimeMin).setStartTimeMax(startTimeMax).build();

    FindTracesRequest query = FindTracesRequest.newBuilder().setQuery(queryParameters).build();

    final List<TracesData> tracesList = new LinkedList<>();
    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
      Iterator<TracesData> traces = queryService.findTraces(query);
      while (traces.hasNext()) {
        tracesList.add(traces.next());
      }
      return !tracesList.isEmpty();
    });
    return tracesList;
  }

  public static List<ByteString> verifySpans(List<TracesData> traces, SpanKind spanKind,
      List<KeyValue> expectedSpanTags, int expectedSpans) {
    int spansCount = 0;
    List<ByteString> spanIds = new ArrayList<>(expectedSpans);
    for (TracesData trace : traces) {
      for (ResourceSpans resourceSpans : trace.getResourceSpansList()) {
        for (ScopeSpans scopeSpans : resourceSpans.getScopeSpansList()) {
          for (Span span : scopeSpans.getSpansList()) {
            if (span.getKind() == spanKind && span.getAttributesList()
                .containsAll(expectedSpanTags)) {
              spanIds.add(span.getSpanId());
              spansCount++;
            }
          }
        }
      }
    }

    if (spansCount != expectedSpans) {
      fail("Was expecting " + expectedSpans + " spans, but actual has " + spansCount);
    }

    return spanIds;
  }

  private static int countSpans(TracesData tracesData) {
    int count = 0;
    for (ResourceSpans resourceSpans : tracesData.getResourceSpansList()) {
      for (ScopeSpans scopeSpans : resourceSpans.getScopeSpansList()) {
        count += scopeSpans.getSpansCount();
      }
    }
    return count;
  }
}