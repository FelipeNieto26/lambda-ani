package com.certicamara.lambda.infrastructure.persistance;

import com.certicamara.lambda.domain.persistance.ILogsRepositoryPort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@ApplicationScoped
public class LogsRepositoryAdapter implements ILogsRepositoryPort {

    private static final Logger logger = LoggerFactory.getLogger(LogsRepositoryAdapter.class);
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private static final String TABLE_NAME = "ani-file-uploads";

    public LogsRepositoryAdapter() {
        this("us-east-2");
    }

    public LogsRepositoryAdapter(String region) {
        logger.info("Inicializando LogsRepositoryAdapter con región: {}", region);

        var httpClient = NettyNioAsyncHttpClient.builder()
                .connectionTimeout(Duration.ofSeconds(60))
                .readTimeout(Duration.ofSeconds(60))
                .writeTimeout(Duration.ofSeconds(60))
                .build();

        this.dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .httpClient(httpClient)
                .build();

        logger.info("DynamoDbAsyncClient inicializado correctamente con NettyNioAsyncHttpClient");
    }

    @Override
    public Uni<Void> updateStatusLog(String id, String status) {
        logger.info("=== INICIANDO ACTUALIZACIÓN EN DYNAMODB ===");
        logger.info("UUID: {}", id);
        logger.info("Estado: {}", status);
        logger.info("Tabla: {}", TABLE_NAME);

        Map<String, AttributeValue> key = Map.of(
                "uid", AttributeValue.builder().s(id).build()
        );

        String updateExpression = "SET #estado = :estado";

        Map<String, String> expressionAttributeNames = Map.of(
                "#estado", "estado"
        );

        Map<String, AttributeValue> expressionAttributeValues = Map.of(
                ":estado", AttributeValue.builder().s(status).build()
        );

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(TABLE_NAME)
                .key(key)
                .updateExpression(updateExpression)
                .expressionAttributeNames(expressionAttributeNames)
                .expressionAttributeValues(expressionAttributeValues)
                .build();

        logger.info("Request construido - Clave: uid={}, UpdateExpression: {}", id, updateExpression);

        return Uni.createFrom().completionStage(
                dynamoDbAsyncClient.updateItem(request)
        ).onItem().invoke(response -> {
            logger.info("✅ DynamoDB UpdateItem completado exitosamente");
            logger.info("   UUID: {}", id);
            logger.info("   Estado nuevo: {}", status);
            logger.info("   Response: {}", response);
        }).onFailure().invoke(throwable -> {
            logger.error("❌ ERROR EN DYNAMODB UPDATE");
            logger.error("   UUID: {}", id);
            logger.error("   Estado intentado: {}", status);
            logger.error("   Tipo de error: {}", throwable.getClass().getName());
            logger.error("   Mensaje: {}", throwable.getMessage());
            logger.error("   Stack trace:", throwable);
        }).replaceWithVoid();
    }

    @Override
    public Uni<Void> addLog(String id, String typeLog, String message) {

        Map<String, AttributeValue> newLogEntry = Map.of(
                "type", AttributeValue.builder().s(typeLog).build(),
                "message", AttributeValue.builder().s(message).build(),
                "timeStamp", AttributeValue.builder().s(Instant.now().toString()).build()
        );

        AttributeValue logListValue = AttributeValue.builder()
                .l(AttributeValue.builder().m(newLogEntry).build())
                .build();

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(TABLE_NAME)
                .key(Map.of("uid", AttributeValue.builder().s(id).build()))
                .updateExpression("SET #logs = list_append(if_not_exists(#logs, :emptyList), :newEntry)")
                .expressionAttributeNames(Map.of(
                        "#logs", "logsParticionArchivos"
                ))
                .expressionAttributeValues(Map.of(
                        ":newEntry", logListValue,
                        ":emptyList", AttributeValue.builder().l(List.of()).build()
                ))
                .build();

        return Uni.createFrom().completionStage(
                dynamoDbAsyncClient.updateItem(request)
        ).replaceWithVoid();
    }
}