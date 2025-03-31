package io.jaegertracing.api_v3;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler",
    comments = "Source: api_v3/query_service.proto")
public final class QueryServiceGrpc {

  private QueryServiceGrpc() {}

  public static final String SERVICE_NAME = "jaeger.api_v3.QueryService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.jaegertracing.api_v3.QueryServiceOuterClass.GetTraceRequest,
      io.opentelemetry.proto.trace.v1.TracesData> getGetTraceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTrace",
      requestType = io.jaegertracing.api_v3.QueryServiceOuterClass.GetTraceRequest.class,
      responseType = io.opentelemetry.proto.trace.v1.TracesData.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<io.jaegertracing.api_v3.QueryServiceOuterClass.GetTraceRequest,
      io.opentelemetry.proto.trace.v1.TracesData> getGetTraceMethod() {
    io.grpc.MethodDescriptor<io.jaegertracing.api_v3.QueryServiceOuterClass.GetTraceRequest, io.opentelemetry.proto.trace.v1.TracesData> getGetTraceMethod;
    if ((getGetTraceMethod = QueryServiceGrpc.getGetTraceMethod) == null) {
      synchronized (QueryServiceGrpc.class) {
        if ((getGetTraceMethod = QueryServiceGrpc.getGetTraceMethod) == null) {
          QueryServiceGrpc.getGetTraceMethod = getGetTraceMethod =
              io.grpc.MethodDescriptor.<io.jaegertracing.api_v3.QueryServiceOuterClass.GetTraceRequest, io.opentelemetry.proto.trace.v1.TracesData>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTrace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.jaegertracing.api_v3.QueryServiceOuterClass.GetTraceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.opentelemetry.proto.trace.v1.TracesData.getDefaultInstance()))
              .setSchemaDescriptor(new QueryServiceMethodDescriptorSupplier("GetTrace"))
              .build();
        }
      }
    }
    return getGetTraceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.jaegertracing.api_v3.QueryServiceOuterClass.FindTracesRequest,
      io.opentelemetry.proto.trace.v1.TracesData> getFindTracesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "FindTraces",
      requestType = io.jaegertracing.api_v3.QueryServiceOuterClass.FindTracesRequest.class,
      responseType = io.opentelemetry.proto.trace.v1.TracesData.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<io.jaegertracing.api_v3.QueryServiceOuterClass.FindTracesRequest,
      io.opentelemetry.proto.trace.v1.TracesData> getFindTracesMethod() {
    io.grpc.MethodDescriptor<io.jaegertracing.api_v3.QueryServiceOuterClass.FindTracesRequest, io.opentelemetry.proto.trace.v1.TracesData> getFindTracesMethod;
    if ((getFindTracesMethod = QueryServiceGrpc.getFindTracesMethod) == null) {
      synchronized (QueryServiceGrpc.class) {
        if ((getFindTracesMethod = QueryServiceGrpc.getFindTracesMethod) == null) {
          QueryServiceGrpc.getFindTracesMethod = getFindTracesMethod =
              io.grpc.MethodDescriptor.<io.jaegertracing.api_v3.QueryServiceOuterClass.FindTracesRequest, io.opentelemetry.proto.trace.v1.TracesData>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "FindTraces"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.jaegertracing.api_v3.QueryServiceOuterClass.FindTracesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.opentelemetry.proto.trace.v1.TracesData.getDefaultInstance()))
              .setSchemaDescriptor(new QueryServiceMethodDescriptorSupplier("FindTraces"))
              .build();
        }
      }
    }
    return getFindTracesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesRequest,
      io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesResponse> getGetServicesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetServices",
      requestType = io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesRequest.class,
      responseType = io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesRequest,
      io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesResponse> getGetServicesMethod() {
    io.grpc.MethodDescriptor<io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesRequest, io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesResponse> getGetServicesMethod;
    if ((getGetServicesMethod = QueryServiceGrpc.getGetServicesMethod) == null) {
      synchronized (QueryServiceGrpc.class) {
        if ((getGetServicesMethod = QueryServiceGrpc.getGetServicesMethod) == null) {
          QueryServiceGrpc.getGetServicesMethod = getGetServicesMethod =
              io.grpc.MethodDescriptor.<io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesRequest, io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetServices"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryServiceMethodDescriptorSupplier("GetServices"))
              .build();
        }
      }
    }
    return getGetServicesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsRequest,
      io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsResponse> getGetOperationsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetOperations",
      requestType = io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsRequest.class,
      responseType = io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsRequest,
      io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsResponse> getGetOperationsMethod() {
    io.grpc.MethodDescriptor<io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsRequest, io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsResponse> getGetOperationsMethod;
    if ((getGetOperationsMethod = QueryServiceGrpc.getGetOperationsMethod) == null) {
      synchronized (QueryServiceGrpc.class) {
        if ((getGetOperationsMethod = QueryServiceGrpc.getGetOperationsMethod) == null) {
          QueryServiceGrpc.getGetOperationsMethod = getGetOperationsMethod =
              io.grpc.MethodDescriptor.<io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsRequest, io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetOperations"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryServiceMethodDescriptorSupplier("GetOperations"))
              .build();
        }
      }
    }
    return getGetOperationsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static QueryServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<QueryServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<QueryServiceStub>() {
        @java.lang.Override
        public QueryServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new QueryServiceStub(channel, callOptions);
        }
      };
    return QueryServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static QueryServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<QueryServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<QueryServiceBlockingStub>() {
        @java.lang.Override
        public QueryServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new QueryServiceBlockingStub(channel, callOptions);
        }
      };
    return QueryServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static QueryServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<QueryServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<QueryServiceFutureStub>() {
        @java.lang.Override
        public QueryServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new QueryServiceFutureStub(channel, callOptions);
        }
      };
    return QueryServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class QueryServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * GetTrace returns a single trace.
     * Note that the JSON response over HTTP is wrapped into result envelope "{"result": ...}"
     * It means that the JSON response cannot be directly unmarshalled using JSONPb.
     * This can be fixed by first parsing into user-defined envelope with standard JSON library
     * or string manipulation to remove the envelope. Alternatively generate objects using OpenAPI.
     * </pre>
     */
    public void getTrace(io.jaegertracing.api_v3.QueryServiceOuterClass.GetTraceRequest request,
        io.grpc.stub.StreamObserver<io.opentelemetry.proto.trace.v1.TracesData> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTraceMethod(), responseObserver);
    }

    /**
     * <pre>
     * FindTraces searches for traces.
     * See GetTrace for JSON unmarshalling.
     * </pre>
     */
    public void findTraces(io.jaegertracing.api_v3.QueryServiceOuterClass.FindTracesRequest request,
        io.grpc.stub.StreamObserver<io.opentelemetry.proto.trace.v1.TracesData> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFindTracesMethod(), responseObserver);
    }

    /**
     * <pre>
     * GetServices returns service names.
     * </pre>
     */
    public void getServices(io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesRequest request,
        io.grpc.stub.StreamObserver<io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetServicesMethod(), responseObserver);
    }

    /**
     * <pre>
     * GetOperations returns operation names.
     * </pre>
     */
    public void getOperations(io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsRequest request,
        io.grpc.stub.StreamObserver<io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetOperationsMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetTraceMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                io.jaegertracing.api_v3.QueryServiceOuterClass.GetTraceRequest,
                io.opentelemetry.proto.trace.v1.TracesData>(
                  this, METHODID_GET_TRACE)))
          .addMethod(
            getFindTracesMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                io.jaegertracing.api_v3.QueryServiceOuterClass.FindTracesRequest,
                io.opentelemetry.proto.trace.v1.TracesData>(
                  this, METHODID_FIND_TRACES)))
          .addMethod(
            getGetServicesMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesRequest,
                io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesResponse>(
                  this, METHODID_GET_SERVICES)))
          .addMethod(
            getGetOperationsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsRequest,
                io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsResponse>(
                  this, METHODID_GET_OPERATIONS)))
          .build();
    }
  }

  /**
   */
  public static final class QueryServiceStub extends io.grpc.stub.AbstractAsyncStub<QueryServiceStub> {
    private QueryServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected QueryServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new QueryServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * GetTrace returns a single trace.
     * Note that the JSON response over HTTP is wrapped into result envelope "{"result": ...}"
     * It means that the JSON response cannot be directly unmarshalled using JSONPb.
     * This can be fixed by first parsing into user-defined envelope with standard JSON library
     * or string manipulation to remove the envelope. Alternatively generate objects using OpenAPI.
     * </pre>
     */
    public void getTrace(io.jaegertracing.api_v3.QueryServiceOuterClass.GetTraceRequest request,
        io.grpc.stub.StreamObserver<io.opentelemetry.proto.trace.v1.TracesData> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getGetTraceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * FindTraces searches for traces.
     * See GetTrace for JSON unmarshalling.
     * </pre>
     */
    public void findTraces(io.jaegertracing.api_v3.QueryServiceOuterClass.FindTracesRequest request,
        io.grpc.stub.StreamObserver<io.opentelemetry.proto.trace.v1.TracesData> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getFindTracesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * GetServices returns service names.
     * </pre>
     */
    public void getServices(io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesRequest request,
        io.grpc.stub.StreamObserver<io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetServicesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * GetOperations returns operation names.
     * </pre>
     */
    public void getOperations(io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsRequest request,
        io.grpc.stub.StreamObserver<io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetOperationsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class QueryServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<QueryServiceBlockingStub> {
    private QueryServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected QueryServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new QueryServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * GetTrace returns a single trace.
     * Note that the JSON response over HTTP is wrapped into result envelope "{"result": ...}"
     * It means that the JSON response cannot be directly unmarshalled using JSONPb.
     * This can be fixed by first parsing into user-defined envelope with standard JSON library
     * or string manipulation to remove the envelope. Alternatively generate objects using OpenAPI.
     * </pre>
     */
    public java.util.Iterator<io.opentelemetry.proto.trace.v1.TracesData> getTrace(
        io.jaegertracing.api_v3.QueryServiceOuterClass.GetTraceRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getGetTraceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * FindTraces searches for traces.
     * See GetTrace for JSON unmarshalling.
     * </pre>
     */
    public java.util.Iterator<io.opentelemetry.proto.trace.v1.TracesData> findTraces(
        io.jaegertracing.api_v3.QueryServiceOuterClass.FindTracesRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getFindTracesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * GetServices returns service names.
     * </pre>
     */
    public io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesResponse getServices(io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetServicesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * GetOperations returns operation names.
     * </pre>
     */
    public io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsResponse getOperations(io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetOperationsMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class QueryServiceFutureStub extends io.grpc.stub.AbstractFutureStub<QueryServiceFutureStub> {
    private QueryServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected QueryServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new QueryServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * GetServices returns service names.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesResponse> getServices(
        io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetServicesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * GetOperations returns operation names.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsResponse> getOperations(
        io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetOperationsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_TRACE = 0;
  private static final int METHODID_FIND_TRACES = 1;
  private static final int METHODID_GET_SERVICES = 2;
  private static final int METHODID_GET_OPERATIONS = 3;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final QueryServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(QueryServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_TRACE:
          serviceImpl.getTrace((io.jaegertracing.api_v3.QueryServiceOuterClass.GetTraceRequest) request,
              (io.grpc.stub.StreamObserver<io.opentelemetry.proto.trace.v1.TracesData>) responseObserver);
          break;
        case METHODID_FIND_TRACES:
          serviceImpl.findTraces((io.jaegertracing.api_v3.QueryServiceOuterClass.FindTracesRequest) request,
              (io.grpc.stub.StreamObserver<io.opentelemetry.proto.trace.v1.TracesData>) responseObserver);
          break;
        case METHODID_GET_SERVICES:
          serviceImpl.getServices((io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesRequest) request,
              (io.grpc.stub.StreamObserver<io.jaegertracing.api_v3.QueryServiceOuterClass.GetServicesResponse>) responseObserver);
          break;
        case METHODID_GET_OPERATIONS:
          serviceImpl.getOperations((io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsRequest) request,
              (io.grpc.stub.StreamObserver<io.jaegertracing.api_v3.QueryServiceOuterClass.GetOperationsResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class QueryServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    QueryServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.jaegertracing.api_v3.QueryServiceOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("QueryService");
    }
  }

  private static final class QueryServiceFileDescriptorSupplier
      extends QueryServiceBaseDescriptorSupplier {
    QueryServiceFileDescriptorSupplier() {}
  }

  private static final class QueryServiceMethodDescriptorSupplier
      extends QueryServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    QueryServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (QueryServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new QueryServiceFileDescriptorSupplier())
              .addMethod(getGetTraceMethod())
              .addMethod(getFindTracesMethod())
              .addMethod(getGetServicesMethod())
              .addMethod(getGetOperationsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
