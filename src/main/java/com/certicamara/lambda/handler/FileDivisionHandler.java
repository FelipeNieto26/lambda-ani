package com.certicamara.lambda.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
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

@Named("fileDivision")
@ApplicationScoped
public class FileDivisionHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private FileDivisionService fileDivisionService;
    private ObjectMapper objectMapper;
    private String bucketNameValue;
    private ILogsRepositoryPort fileStatusRepository;

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
            System.out.println("[INFO] Bucket Name: " + bucketNameValue + " (isPresent: " + (bucketName != null && bucketName.isPresent()) + ")");
            System.out.println("[INFO] Region: " + regionValue + " (isPresent: " + (region != null && region.isPresent()) + ")");
            System.out.println("[INFO] Expected Records: " + expectedRecordsValue + " (isPresent: " + (expectedRecords != null && expectedRecords.isPresent()) + ")");
            System.out.println("[INFO] Records Per Chunk: " + recordsPerChunkValue + " (isPresent: " + (recordsPerChunk != null && recordsPerChunk.isPresent()) + ")");
            System.out.println("[INFO] Output Prefix: " + outputPrefixValue + " (isPresent: " + (outputPrefix != null && outputPrefix.isPresent()) + ")");

            S3Service s3Service = new S3Service(bucketNameValue, regionValue);
            
            // Inicializar SQS service para envío de mensajes por chunk
            String queueUrl = "https://sqs.us-east-2.amazonaws.com/343218178755/file-processing-queue";
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
            
            // Inicializar repositorio de logs manualmente (CDI no funciona en Lambda)
            System.out.println("[INFO] Inicializando LogsRepository manualmente...");
            try {
                this.fileStatusRepository = new com.certicamara.lambda.infrastructure.persistance.LogsRepositoryAdapter(regionValue);
                System.out.println("[INFO] ✅ LogsRepository inicializado correctamente");
            } catch (Exception e) {
                System.out.println("[ERROR] ❌ Error inicializando LogsRepository: " + e.getMessage());
                e.printStackTrace();
            }

            System.out.println("[INFO] === Servicios inicializados correctamente ===");
        }
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        System.out.println("[INFO] === INICIANDO HANDLER - DIVISIÓN DE ARCHIVOS ===");

        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "*");
        response.setHeaders(headers);

        try {
            ensureInitialized();

            Map<String, String> queryParams = request.getQueryStringParameters();
            if (queryParams == null || !queryParams.containsKey("uuid")) {
                System.out.println("[WARN] Parámetro 'uuid' no proporcionado");
                response.setStatusCode(400);
                response.setBody("{\"error\":\"El parámetro uuid es requerido\"}");
                return response;
            }
            String uuid = queryParams.get("uuid");
            if (uuid == null || uuid.trim().isEmpty()) {
                System.out.println("[WARN] Parámetro 'uuid' está vacío");
                response.setStatusCode(400);
                response.setBody("{\"error\":\"El parámetro uuid no puede estar vacío\"}");
                return response;
            }
            System.out.println("[INFO] UUID recibido: " + uuid);

            String fileKey = findFileInDirectory(uuid);
            System.out.println("[INFO] ✅ Archivo encontrado: " + fileKey);

            String fileType = fileKey.toLowerCase().endsWith(".xlsx") ? "XLSX (Excel)" : "CSV";
            System.out.println("[INFO] 📄 Tipo de archivo: " + fileType);

            String outputFolder = uuid + "/output/";
            System.out.println("[INFO] 📁 Carpeta de salida configurada: " + outputFolder);
            
            // ===== ACTUALIZAR ESTADO A "EN PROGRESO" EN DYNAMODB =====
            System.out.println("[INFO] ==========================================");
            System.out.println("[INFO] 📝 ACTUALIZANDO ESTADO A EN PROGRESO");
            System.out.println("[INFO] ==========================================");
            System.out.println("[INFO] UUID a actualizar: " + uuid);
            System.out.println("[INFO] Longitud UUID: " + uuid.length());
            System.out.println("[INFO] UUID trim: '" + uuid.trim() + "'");
            
            if (fileStatusRepository == null) {
                System.out.println("[ERROR] ❌❌❌ fileStatusRepository es NULL - No se inyectó correctamente");
                System.out.println("[ERROR] ❌ CDI no funcionó - verificar @ApplicationScoped en LogsRepositoryAdapter");
                System.out.println("[ERROR] Saltando actualización de DynamoDB");
            } else {
                System.out.println("[INFO] ✅ fileStatusRepository está inyectado correctamente");
                System.out.println("[INFO] Clase: " + fileStatusRepository.getClass().getName());
                
                try {
                    System.out.println("[INFO] >>> Llamando a updateStatusLog('" + uuid + "', 'EN PROGRESO')...");
                    
                    long updateStart = System.currentTimeMillis();
                    fileStatusRepository.updateStatusLog(uuid, "EN PROGRESO")
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
                    
                    // Imprimir causa raíz si existe
                    Throwable cause = dbError.getCause();
                    while (cause != null) {
                        System.out.println("[ERROR] Causa: " + cause.getClass().getName() + " - " + cause.getMessage());
                        cause = cause.getCause();
                    }
                }
            }
            
            System.out.println("[INFO] ==========================================");
            System.out.println("[INFO] 🚀 Iniciando procesamiento del archivo...");

            long processingStart = System.currentTimeMillis();
            DivisionResult result = fileDivisionService.processDivisionWithCustomOutput(fileKey, outputFolder, uuid);
            long processingTime = System.currentTimeMillis() - processingStart;

            System.out.println("[INFO] ⏱️ Tiempo de procesamiento total: " + processingTime + " ms (" + (processingTime / 1000) + " segundos)");


            if (result == null || !result.isSuccess()) {
                String errorMessage = (result != null && result.getMessage() != null)
                        ? result.getMessage()
                        : "Error desconocido durante la división del archivo.";

                System.out.println("[ERROR] === LA DIVISIÓN FALLÓ ===");
                System.out.println("[ERROR] Mensaje de error: " + errorMessage);

                throw new RuntimeException(errorMessage);
            }

            System.out.println("[INFO] === DIVISIÓN COMPLETADA ===");
            System.out.println("[INFO] Archivos generados: " + result.getTotalChunks());
            System.out.println("[INFO] Total registros: " + result.getTotalRecords());
            System.out.println("[INFO] ✅ Mensajes SQS enviados automáticamente por cada chunk generado");
            
            // Responder con 202 Accepted indicando que el procesamiento está completo
            response.setStatusCode(202);
            response.setBody(String.format("{\"message\":\"File processed successfully\",\"uuid\":\"%s\",\"chunks\":%d,\"records\":%d}", 
                uuid, result.getTotalChunks(), result.getTotalRecords()));
            
            response.setIsBase64Encoded(false);
            System.out.println("[INFO] === RETORNANDO RESPUESTA 202 ACCEPTED ===");
            return response;
        } catch (Exception e) {
            System.out.println("[ERROR] === ERROR EN EL HANDLER ===");
            System.out.println("[ERROR] Mensaje: " + e.getMessage());
            System.out.println("[ERROR] Tipo: " + e.getClass().getName());

            boolean isTimeout = e.getMessage() != null &&
                    (e.getMessage().contains("timeout") || e.getMessage().contains("timed out"));

            if (isTimeout) {
                System.out.println("[ERROR] ⚠️ TIMEOUT DETECTADO - El archivo es muy grande o el procesamiento tomó demasiado tiempo");
                System.out.println("[ERROR] 💡 Sugerencias: Aumentar el timeout de Lambda o reducir el tamaño del archivo");
            }

            response.setStatusCode(500);
            String safeErrorMessage = e.getMessage() != null ? e.getMessage().replace("\"", "'") : "Error desconocido";
            response.setBody("{\"error\":\"Error interno: " + safeErrorMessage + "\"}");
            return response;
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

    private APIGatewayProxyResponseEvent createErrorResponse(int statusCode, String message) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(statusCode);

        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "*");
        response.setHeaders(headers);

        try {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }

            Map<String, Object> errorBody = new HashMap<>();
            errorBody.put("success", false);
            errorBody.put("error", message);
            response.setBody(objectMapper.writeValueAsString(errorBody));
        } catch (Exception e) {
            System.out.println("[ERROR] Error al serializar respuesta de error: " + e.getMessage());
            response.setBody("{\"success\":false,\"error\":\"" + message.replace("\"", "\\\"") + "\"}");
        }
        return response;
    }
}