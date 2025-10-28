package com.certicamara.lambda.infrastructure;

import com.certicamara.lambda.domain.persistance.ILogsRepositoryPort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@ApplicationScoped
public class LogsRepositoryAdapter implements ILogsRepositoryPort {

    private static final Logger logger = LoggerFactory.getLogger(LogsRepositoryAdapter.class);
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private static final String TABLE_NAME = "ani-file-uploads";

    public LogsRepositoryAdapter(@ConfigProperty(name = "aws.region") String region) {
        logger.info("Inicializando LogsRepositoryAdapter con región: {}", region);
        this.dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    @Override
    public Uni<Void> updateStatusLog(String id, String status) {
        logger.info("Actualizando estado en DynamoDB - UUID: {}, Estado: {}", id, status);
        
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

        return Uni.createFrom().completionStage(
                dynamoDbAsyncClient.updateItem(request)
        ).onItem().invoke(() -> 
            logger.info("✅ Estado actualizado exitosamente en DynamoDB - UUID: {}, Estado: {}", id, status)
        ).onFailure().invoke(throwable ->
            logger.error("❌ Error al actualizar estado en DynamoDB - UUID: {}, Error: {}", id, throwable.getMessage())
        ).replaceWithVoid();
    }
}

