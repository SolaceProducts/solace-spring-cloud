package com.solace.spring.cloud.stream.binder.util;

import com.solace.messaging.trace.propagation.Baggage;
import com.solace.messaging.trace.propagation.MessageTracingSupport;
import com.solace.messaging.trace.propagation.TraceContext;
import com.solace.messaging.trace.propagation.TraceContextSetter;
import com.solacesystems.jcsmp.XMLMessage;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;

/**
 * W3C Trace Context Propagation: maps the SMF message's native trace context to and from the
 * {@code traceparent}, {@code tracestate} and {@code baggage} message headers.
 */
final class TraceContextMapper {

  static final String TRACEPARENT = "traceparent";
  static final String TRACESTATE = "tracestate";
  static final String BAGGAGE = "baggage";
  static final Set<String> HEADERS = Set.of(TRACEPARENT, TRACESTATE, BAGGAGE);

  private static final Logger LOGGER = LoggerFactory.getLogger(TraceContextMapper.class);
  private static final HexFormat HEX = HexFormat.of();

  private TraceContextMapper() {
  }

  /**
   * Sets the trace context headers from the message's native trace context. The current hop's
   * context (transport context) is preferred over the original publisher's context (creation
   * context). Baggage is only mapped together with a trace context.
   */
  static void readTraceContext(XMLMessage xmlMessage, AbstractIntegrationMessageBuilder<?> builder,
      Set<String> headerExclusions) {
    if (!(xmlMessage instanceof MessageTracingSupport tracing)) {
      return;
    }
    TraceContext context = currentContext(tracing);
    if (context != null) {
      setHeader(builder, headerExclusions, TRACEPARENT,
          "00-" + HEX.formatHex(context.getTraceIdBytes16()) + "-"
              + HEX.formatHex(context.getSpanIdBytes8()) + "-" + (context.isSampled() ? "01"
              : "00"));
      setHeader(builder, headerExclusions, TRACESTATE, context.getTraceState());
      Baggage baggage = tracing.baggage();
      if (baggage != null) {
        setHeader(builder, headerExclusions, BAGGAGE, baggage.getBaggage());
      }
    }
  }

  private static TraceContext currentContext(MessageTracingSupport tracing) {
    if (isValid(tracing.transportContext())) {
      return tracing.transportContext();
    }
    if (isValid(tracing.creationContext())) {
      return tracing.creationContext();
    }
    return null;
  }

  /**
   * Writes the trace context headers onto the message's native trace context. The trace ID must be
   * written first: JCSMP ignores the trace state and sampled flag until it exists. Baggage is only
   * written together with a valid trace context.
   */
  static void writeTraceContext(XMLMessage xmlMessage, Map<String, Object> headers,
      Set<String> headerExclusions) {
    if (!(xmlMessage instanceof MessageTracingSupport tracing)) {
      return;
    }
    if (!(headerValue(headers, headerExclusions, TRACEPARENT) instanceof String traceparent)) {
      return;
    }
    // Written by a tracing propagator, so only what the fixed-position parse needs is checked,
    // plus the W3C rule that the IDs are not all zeros.
    byte[] traceId = traceparent.length() == 55 ? parseHex(traceparent, 3, 35) : null;
    byte[] spanId = traceId != null ? parseHex(traceparent, 36, 52) : null;
    byte[] flags = spanId != null ? parseHex(traceparent, 53, 55) : null;
    if (!isNonZero(traceId) || !isNonZero(spanId) || flags == null) {
      LOGGER.debug("Ignoring invalid {} header: {}", TRACEPARENT, traceparent);
      return;
    }
    TraceContextSetter setter = tracing.contextSetter();
    setter.setTraceIdBytes16(traceId);
    setter.setSpanIdBytes8(spanId);
    setter.setSampled((flags[0] & 0x01) != 0);
    if (headerValue(headers, headerExclusions, TRACESTATE) instanceof String traceState
        && !traceState.isEmpty()) {
      setter.setTraceState(traceState);
    }
    if (headerValue(headers, headerExclusions, BAGGAGE) instanceof String baggage
        && !baggage.isEmpty()
        && tracing.baggage() != null) {
      tracing.baggage().setBaggage(baggage);
    }
  }

  // Null when the range is not hex.
  private static byte[] parseHex(String value, int fromIndex, int toIndex) {
    try {
      return HEX.parseHex(value, fromIndex, toIndex);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static Object headerValue(Map<String, Object> headers, Set<String> headerExclusions,
      String name) {
    return headerExclusions != null && headerExclusions.contains(name) ? null : headers.get(name);
  }

  private static void setHeader(AbstractIntegrationMessageBuilder<?> builder,
      Set<String> headerExclusions,
      String name, String value) {
    if (value != null && !value.isEmpty() && (headerExclusions == null
        || !headerExclusions.contains(name))) {
      builder.setHeader(name, value);
    }
  }

  private static boolean isValid(TraceContext context) {
    return context != null && isNonZero(context.getTraceIdBytes16()) && isNonZero(
        context.getSpanIdBytes8());
  }

  private static boolean isNonZero(byte[] bytes) {
    if (bytes == null) {
      return false;
    }
    for (byte b : bytes) {
      if (b != 0) {
        return true;
      }
    }
    return false;
  }
}