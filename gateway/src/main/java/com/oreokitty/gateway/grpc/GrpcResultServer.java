package com.oreokitty.gateway.grpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oreokitty.gateway.model.TranslationResult;
import com.oreokitty.gateway.service.TranslationStreamService;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;

@Component
public class GrpcResultServer {

    private static final Logger log = LoggerFactory.getLogger(GrpcResultServer.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final MethodDescriptor.Marshaller<byte[]> RAW = new MethodDescriptor.Marshaller<>() {
        @Override
        public InputStream stream(byte[] value) {
            return new ByteArrayInputStream(value);
        }

        @Override
        public byte[] parse(InputStream stream) {
            try {
                return stream.readAllBytes();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    };

    private final TranslationStreamService streamService;
    private final int port;
    private Server server;

    public GrpcResultServer(TranslationStreamService streamService,
                            @Value("${oreo.grpc.port:9090}") int port) {
        this.streamService = streamService;
        this.port = port;
    }

    @PostConstruct
    public void start() throws IOException {
        if (server != null && !server.isShutdown()) {
            return;
        }
        MethodDescriptor<byte[], byte[]> publishResult = MethodDescriptor.<byte[], byte[]>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("oreo.gateway.ResultService/PublishResult")
                .setRequestMarshaller(RAW)
                .setResponseMarshaller(RAW)
                .build();

        ServerServiceDefinition service = ServerServiceDefinition
                .builder("oreo.gateway.ResultService")
                .addMethod(publishResult, ServerCalls.asyncUnaryCall(
                        new ServerCalls.UnaryMethod<byte[], byte[]>() {
                            @Override
                            public void invoke(byte[] request, StreamObserver<byte[]> responseObserver) {
                                byte[] response = handlePublishResult(request);
                                responseObserver.onNext(response);
                                responseObserver.onCompleted();
                            }
                        }))
                .build();

        server = ServerBuilder.forPort(port)
                .addService(service)
                .build()
                .start();
        log.info("gRPC result server started on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdownNow, "grpc-shutdown"));
    }

    private byte[] handlePublishResult(byte[] payload) {
        try {
            Map<?, ?> json = MAPPER.readValue(payload, Map.class);
            TranslationResult result = new TranslationResult(
                    String.valueOf(json.get("fileId")),
                    String.valueOf(json.get("intent")),
                    ((Number) json.get("confidence")).doubleValue());
            streamService.complete(result.fileId(), result);
            return "{}".getBytes();
        } catch (Exception e) {
            log.error("Failed to process ML prediction callback", e);
            return "{}".getBytes();
        }
    }

    @PreDestroy
    public void stop() {
        if (server != null) {
            server.shutdownNow();
        }
    }
}
