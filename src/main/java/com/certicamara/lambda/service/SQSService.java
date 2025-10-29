package com.certicamara.lambda.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
public class SQSService {

    private static final Logger logger = LoggerFactory.getLogger(SQSService.class);

    private SqsClient sqsClient;
    private String queueUrl;
    
    @ConfigProperty(name = "aws.sqs.queue.url")
    String sqsQueueUrl;
    
    @ConfigProperty(name = "aws.region")
    String region;

    public SQSService() {
    }
    
    public SQSService(String queueUrl, String region) {
        this.queueUrl = queueUrl;
        this.region = region;
        initializeSqsClient();
    }
    
    private void initializeSqsClient() {
        if (sqsClient == null) {
            sqsClient = SqsClient.builder()
                    .region(Region.of(region))
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();
            logger.info("SqsClient inicializado - Queue URL: {}, Region: {}", queueUrl, region);
        }
    }

    /**
     * Envía un mensaje a la cola SQS
     */
    public void sendMessage(String filename, String uuid) {
        try {
            initializeSqsClient();
            
            Map<String, String> messageBody = new HashMap<>();
            messageBody.put("filename", filename);
            messageBody.put("uuid", uuid);
            
            ObjectMapper mapper = new ObjectMapper();
            ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
            String messageJson = writer.writeValueAsString(messageBody);
            
            logger.info("JSON que se enviará a SQS:\n{}", messageJson);
            
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageJson)
                    .build();
            
            SendMessageResponse response = sqsClient.sendMessage(sendMessageRequest);
            
            logger.info("Mensaje enviado a SQS exitosamente - MessageId: {}, Filename: {}, UUID: {}", 
                response.messageId(), filename, uuid);
            
        } catch (Exception e) {
            logger.error("Error al enviar mensaje a SQS: {}", e.getMessage(), e);
            throw new RuntimeException("Error al enviar mensaje a SQS: " + e.getMessage(), e);
        }
    }

    /**
     * Envía un mensaje de chunk a la cola SQS (nuevo formato)
     * @param bucket Nombre del bucket S3
     * @param key Ruta completa del chunk en S3 (ej: "uuid/output/chunk_0001.csv")
     * @param sizeBatch Tamaño del batch (siempre 100)
     * @param idLog UUID del log en DynamoDB
     */
    public void sendChunkMessage(String bucket, String key, int sizeBatch, String idLog) {
        try {
            initializeSqsClient();
            
            Map<String, Object> messageBody = new HashMap<>();
            messageBody.put("bucket", bucket);
            messageBody.put("key", key);
            messageBody.put("sizeBatch", sizeBatch);
            messageBody.put("idLog", idLog);
            
            ObjectMapper mapper = new ObjectMapper();
            ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
            String messageJson = writer.writeValueAsString(messageBody);
            
            logger.info("JSON de chunk que se enviará a SQS:\n{}", messageJson);
            
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageJson)
                    .build();
            
            SendMessageResponse response = sqsClient.sendMessage(sendMessageRequest);
            
            logger.info("Mensaje de chunk enviado a SQS exitosamente - MessageId: {}, Bucket: {}, Key: {}, IdLog: {}", 
                response.messageId(), bucket, key, idLog);
            
        } catch (Exception e) {
            logger.error("Error al enviar mensaje de chunk a SQS: {}", e.getMessage(), e);
            throw new RuntimeException("Error al enviar mensaje de chunk a SQS: " + e.getMessage(), e);
        }
    }
}

