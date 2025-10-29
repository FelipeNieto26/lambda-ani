package com.certicamara.lambda.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.certicamara.lambda.domain.persistance.ILogsRepositoryPort;
import com.certicamara.lambda.model.DivisionResult;
import com.certicamara.lambda.service.FileDivisionService;
import com.certicamara.lambda.service.S3Service;
import com.certicamara.lambda.service.SQSService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.s3.model.ObjectStorageClass;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Handler para procesamiento de archivos mediante eventos SQS
 * 
 * Flujo:
 * 1. Cliente envía mensaje a SQS Input Queue con {"uuid": "abc123"}
 * 2. Lambda se dispara automáticamente
 * 3. Lambda procesa el archivo (buscar, dividir, subir chunks)
 * 4. Lambda envía mensaje a SQS Output Queue cuando termina
 * 
 * Ventajas:
 * - Sin timeout de API Gateway (29 seg)
 * - Retry automático si falla
 * - Procesamiento batch (múltiples mensajes)
 * - Dead Letter Queue para errores
 */
@Named("fileDivisionSQS")
@ApplicationScoped
public class FileDivisionHandlerSQS implements RequestHandler<SQSEvent, Void> {

    private FileDivisionService fileDivisionService;
    private ObjectMapper objectMapper;
    private String bucketNameValue;
    private ILogsRepositoryPort logsRepository;

    @ConfigProperty(name = "aws.s3.file.name")
    Optional<String> fileName;

    @ConfigProperty(name = "aws.s3.default.prefix")
    Optional<String> defaultPrefix;

    @ConfigProperty(name = "file.division.expected.records")
    Optional<Integer> expectedRecords;

    @ConfigProperty(name = "file.division.records.per.chunk")
    Optional<Integer> recordsPerChunk;

    @ConfigProperty(name = "aws.s3.output.prefix")
    Optional<String> outputPrefix;

    @ConfigProperty(name = "aws.s3.bucket.name")
    Optional<String> bucketName;

    @ConfigProperty(name = "aws.region")
    Optional<String> region;
    
    @ConfigProperty(name = "aws.sqs.output.queue.url")
    Optional<String> outputQueueUrl;

    private void ensureInitialized() {
        if (fileDivisionService == null || objectMapper == null) {
            System.out.println("[INFO] === INICIALIZANDO SERVICIOS ===");

            this.objectMapper = new ObjectMapper();

            this.bucketNameValue = bucketName != null ? bucketName.orElse("ani-input-batch") : "ani-input-batch";
            String regionValue = region != null ? region.orElse("us-east-2") : "us-east-2";
            int expectedRecordsValue = expectedRecords != null ? expectedRecords.orElse(1000000) : 1000000;
            int recordsPerChunkValue = recordsPerChunk != null ? recordsPerChunk.orElse(10000) : 10000;
            String outputPrefixValue = outputPrefix != null ? outputPrefix.orElse("output/") : "output/";

            System.out.println("[INFO] === CONFIGURACIÓN DETECTADA ===");
            System.out.println("[INFO] Bucket Name: " + bucketNameValue);
            System.out.println("[INFO] Region: " + regionValue);
            System.out.println("[INFO] Expected Records: " + expectedRecordsValue);
            System.out.println("[INFO] Records Per Chunk: " + recordsPerChunkValue);
            System.out.println("[INFO] Output Prefix: " + outputPrefixValue);

            S3Service s3Service = new S3Service(bucketNameValue, regionValue);
            
            String queueUrl = outputQueueUrl != null && outputQueueUrl.isPresent()
                ? outputQueueUrl.get()
                : "https://sqs.us-east-2.amazonaws.com/343218178755/file-processing-queue";
            SQSService sqsService = new SQSService(queueUrl, regionValue);

            this.fileDivisionService = new FileDivisionService(
                    s3Service,
                    sqsService,
                    expectedRecordsValue,
                    recordsPerChunkValue,
                    outputPrefixValue,
                    bucketNameValue,
                    regionValue
            );
            
            System.out.println("[INFO] Inicializando LogsRepository manualmente...");
            try {
                // Importar la implementación directamente
                this.logsRepository = new com.certicamara.lambda.infrastructure.persistance.LogsRepositoryAdapter(regionValue);
                System.out.println("[INFO] ✅ LogsRepository inicializado correctamente");
            } catch (Exception e) {
                System.out.println("[ERROR] ❌ Error inicializando LogsRepository: " + e.getMessage());
                e.printStackTrace();
            }

            System.out.println("[INFO] === Servicios inicializados correctamente ===");
        }
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        System.out.println("[INFO] ========================================");
        System.out.println("[INFO] === HANDLER SQS - DIVISIÓN DE ARCHIVOS ===");
        System.out.println("[INFO] ========================================");
        System.out.println("[INFO] Function Name: " + context.getFunctionName());
        System.out.println("[INFO] Mensajes recibidos: " + event.getRecords().size());

        ensureInitialized();

        int successCount = 0;
        int failureCount = 0;
        
        for (SQSMessage message : event.getRecords()) {
            try {
                System.out.println("[INFO] ----------------------------------------");
                System.out.println("[INFO] Procesando mensaje " + (successCount + failureCount + 1) + " de " + event.getRecords().size());
                processMessage(message, context);
                successCount++;
                System.out.println("[INFO] ✅ Mensaje procesado exitosamente");
            } catch (Exception e) {
                System.out.println("[ERROR] ❌ Error procesando mensaje: " + message.getMessageId());
                System.out.println("[ERROR] " + e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Error procesando mensaje SQS: " + message.getMessageId(), e);
            }
        }

        System.out.println("[INFO] ========================================");
        System.out.println("[INFO] === RESUMEN DE PROCESAMIENTO ===");
        System.out.println("[INFO] Total mensajes: " + event.getRecords().size());
        System.out.println("[INFO] Exitosos: " + successCount);
        System.out.println("[INFO] Fallidos: " + failureCount);
        System.out.println("[INFO] ========================================");

        return null;
    }

    /**
     * Procesa un mensaje individual de SQS
     */
    private void processMessage(SQSMessage message, Context context) throws Exception {
        System.out.println("[INFO] === PROCESANDO MENSAJE SQS ===");
        System.out.println("[INFO] Message ID: " + message.getMessageId());
        System.out.println("[INFO] Receipt Handle: " + message.getReceiptHandle().substring(0, Math.min(50, message.getReceiptHandle().length())) + "...");

        String messageBody = message.getBody();
        System.out.println("[INFO] Message Body: " + messageBody);

        Map<String, Object> messageData;
        try {
            messageData = objectMapper.readValue(messageBody, Map.class);
        } catch (Exception e) {
            System.out.println("[ERROR] Error parseando JSON del mensaje");
            throw new IllegalArgumentException("Mensaje JSON inválido", e);
        }
        
        String uuid = (String) messageData.get("uuid");
        if (uuid == null || uuid.trim().isEmpty()) {
            System.out.println("[ERROR] UUID no encontrado en mensaje");
            throw new IllegalArgumentException("UUID es requerido en el mensaje. Formato esperado: {\"uuid\": \"abc123\"}");
        }

        System.out.println("[INFO] UUID extraído: " + uuid);

        // ===== ACTUALIZAR ESTADO A "EN PROGRESO" EN DYNAMODB =====
        System.out.println("[INFO] ==========================================");
        System.out.println("[INFO] 📝 ACTUALIZANDO ESTADO A EN PROGRESO");
        System.out.println("[INFO] ==========================================");
        System.out.println("[INFO] UUID a actualizar: " + uuid);
        
        if (logsRepository == null) {
            System.out.println("[ERROR] ❌❌❌ logsRepository es NULL - No se inyectó correctamente");
            System.out.println("[ERROR] Saltando actualización de DynamoDB");
        } else {
            System.out.println("[INFO] ✅ logsRepository está inyectado correctamente");
            System.out.println("[INFO] Clase: " + logsRepository.getClass().getName());
            
            try {
                System.out.println("[INFO] >>> Llamando a updateStatusLog('" + uuid + "', 'EN PROGRESO')...");
                
                long updateStart = System.currentTimeMillis();
                logsRepository.updateStatusLog(uuid, "EN PROGRESO")
                    .await().indefinitely();
                long updateTime = System.currentTimeMillis() - updateStart;
                
                System.out.println("[INFO] ✅✅✅ Estado actualizado a EN PROGRESO EXITOSAMENTE");
                System.out.println("[INFO] Tiempo de actualización: " + updateTime + " ms");
                
            } catch (Exception dbError) {
                System.out.println("[ERROR] ==========================================");
                System.out.println("[ERROR] ❌❌❌ ERROR al actualizar estado en DynamoDB");
                System.out.println("[ERROR] ==========================================");
                System.out.println("[ERROR] UUID intentado: " + uuid);
                System.out.println("[ERROR] Tipo error: " + dbError.getClass().getName());
                System.out.println("[ERROR] Mensaje error: " + dbError.getMessage());
                System.out.println("[ERROR] ==========================================");
                dbError.printStackTrace();
                
                Throwable cause = dbError.getCause();
                while (cause != null) {
                    System.out.println("[ERROR] Causa: " + cause.getClass().getName() + " - " + cause.getMessage());
                    cause = cause.getCause();
                }
            }
        }
        
        System.out.println("[INFO] ==========================================");

        System.out.println("[INFO] 🔍 Buscando archivo en S3...");
        String fileKey = findFileInDirectory(uuid);
        System.out.println("[INFO] ✅ Archivo encontrado: " + fileKey);
        
        String fileType = fileKey.toLowerCase().endsWith(".xlsx") ? "XLSX (Excel)" : "CSV";
        System.out.println("[INFO] 📄 Tipo de archivo: " + fileType);
        
        String outputFolder = uuid + "/output/";
        String filename = fileKey.substring(fileKey.lastIndexOf("/") + 1);
        
        System.out.println("[INFO] Archivo: " + filename);
        System.out.println("[INFO] Output: " + outputFolder);
        
        long processingStartTime = System.currentTimeMillis();
        
        System.out.println("[INFO] 🚀 Iniciando procesamiento del archivo...");
        logsRepository.addLog(uuid, "INFO", "Inicio division del archivo: " + fileKey);
        DivisionResult result = fileDivisionService.processDivisionWithCustomOutput(
            fileKey, 
            outputFolder,
            uuid
        );
        
        long processingTime = System.currentTimeMillis() - processingStartTime;
        System.out.println("[INFO] ⏱️ Tiempo de procesamiento: " + processingTime + " ms (" + 
            (processingTime / 1000) + " segundos)");
        
        if (result == null || !result.isSuccess()) {
            String errorMessage = (result != null && result.getMessage() != null)
                ? result.getMessage()
                : "Error desconocido durante la división del archivo.";
            logsRepository.addLog(uuid, "INFO", "Fallo division del archivo: " + fileKey);

            System.out.println("[ERROR] ❌ === LA DIVISIÓN FALLÓ ===");
            System.out.println("[ERROR] ❌ Error: " + errorMessage);
            System.out.println("[ERROR] ❌ No se enviará mensaje a SQS de salida");
            
            throw new RuntimeException("Error en división: " + errorMessage);
        }
        
        System.out.println("[INFO] ✅ === DIVISIÓN COMPLETADA EXITOSAMENTE ===");
        System.out.println("[INFO] ✅ Archivos generados: " + result.getTotalChunks());
        System.out.println("[INFO] ✅ Total registros: " + result.getTotalRecords());
        System.out.println("[INFO] ✅ Carpeta: " + result.getOutputFolder());
        System.out.println("[INFO] ✅ Mensajes SQS enviados automáticamente por cada chunk generado");
        logsRepository.addLog(uuid, "INFO", "Fin division del archivo: " + fileKey);

        System.out.println("[INFO] ========================================");
        System.out.println("[INFO] === MENSAJE PROCESADO COMPLETAMENTE ===");
        System.out.println("[INFO] === Tiempo total: " + processingTime + " ms ===");
        System.out.println("[INFO] ========================================");
    }

    /**
     * Envía mensaje de éxito a la cola SQS de salida
     */
    private void sendSuccessMessageToSQS(String uuid, String filename, DivisionResult result) {
        try {
            System.out.println("[INFO] 📤 Enviando mensaje a SQS de salida...");
            
            String queueUrl = outputQueueUrl != null && outputQueueUrl.isPresent()
                ? outputQueueUrl.get()
                : "https://sqs.us-east-2.amazonaws.com/343218178755/file-processing-queue";
            
            String regionValue = region != null ? region.orElse("us-east-2") : "us-east-2";
            
            System.out.println("[INFO] Output Queue URL: " + queueUrl);
            
            SQSService sqsService = new SQSService(queueUrl, regionValue);
            sqsService.sendMessage(filename, uuid);
            
            System.out.println("[INFO] ✅ Mensaje enviado a SQS exitosamente");
            System.out.println("[INFO]    - UUID: " + uuid);
            System.out.println("[INFO]    - Filename: " + filename);
            System.out.println("[INFO]    - Chunks: " + result.getTotalChunks());
            System.out.println("[INFO]    - Records: " + result.getTotalRecords());
            
        } catch (Exception sqsError) {
            System.out.println("[ERROR] ❌ ERROR al enviar mensaje a SQS: " + sqsError.getMessage());
            sqsError.printStackTrace();

        }
    }

    /**
     * Busca un archivo CSV o XLSX en el directorio UUID
     */
    private String findFileInDirectory(String uuid) {
        try {
            String folderPath = uuid + "/";
            System.out.println("[INFO] Buscando archivos CSV o XLSX en: " + folderPath);

            var files = fileDivisionService.listFilesInDirectory(folderPath);

            if (files.isEmpty()) {
                throw new RuntimeException("No se encontraron archivos en el directorio: " + folderPath);
            }

            System.out.println("[INFO] Total archivos encontrados: " + files.size());

            // Buscar primer archivo CSV o XLSX (excluyendo carpetas)
            for (var file : files) {
                String fileName = file.key();

                System.out.println("[INFO] Evaluando archivo: " + fileName + " (Clase: " + file.storageClassAsString() + ", Tamaño: " + file.size() + ")");

                if (fileName.endsWith("/")) {
                    System.out.println("[DEBUG] Saltando carpeta: " + fileName);
                    continue;
                }

                String lowerFileName = fileName.toLowerCase();
                if (lowerFileName.endsWith(".csv") || lowerFileName.endsWith(".xlsx")) {
                    System.out.println("[INFO] ✅ Archivo seleccionado: " + fileName);

                    if (file.storageClass() != null &&
                            (file.storageClass().equals(ObjectStorageClass.GLACIER) ||
                                    file.storageClass().equals(ObjectStorageClass.DEEP_ARCHIVE))) {

                        System.out.println("[ERROR] ¡ERROR! El archivo seleccionado está en " + file.storageClassAsString() + " y no puede ser procesado directamente.");
                        throw new RuntimeException("El archivo '" + fileName + "' está en " + file.storageClassAsString() +
                                " y debe ser restaurado antes de procesar.");
                    }

                    return fileName;
                }

                System.out.println("[DEBUG] Archivo no es CSV/XLSX, saltando: " + fileName);
            }

            throw new RuntimeException("No se encontró ningún archivo CSV o XLSX en: " + folderPath);

        } catch (Exception e) {
            System.out.println("[ERROR] Error al buscar archivo en directorio: " + e.getMessage());
            throw new RuntimeException("Error al buscar archivo: " + e.getMessage(), e);
        }
    }
}

