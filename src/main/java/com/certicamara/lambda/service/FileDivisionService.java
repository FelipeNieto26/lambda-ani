package com.certicamara.lambda.service;

import com.certicamara.lambda.model.DivisionResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.*;

@ApplicationScoped
public class FileDivisionService {

    private static final Logger logger = LoggerFactory.getLogger(FileDivisionService.class);

    @Inject
    S3Service s3Service;

    @ConfigProperty(name = "file.division.expected.records")
    int expectedRecords;

    @ConfigProperty(name = "file.division.records.per.chunk")
    int recordsPerChunk;

    @ConfigProperty(name = "aws.s3.output.prefix")
    String outputPrefix;

    @ConfigProperty(name = "aws.s3.bucket.name")
    String bucketName;

    @ConfigProperty(name = "aws.region")
    String region;

    public String getFileFromS3(String fileKey) {
        try {
            logger.info("Iniciando obtención de archivo: {}", fileKey);
            String content = s3Service.getFileContent(fileKey);
            logger.info("Archivo obtenido exitosamente");
            return content;
        } catch (Exception e) {
            logger.error("Error al obtener archivo: {}", e.getMessage(), e);
            throw new RuntimeException("Error al obtener archivo de S3", e);
        }
    }

    public List<S3Object> listFilesInDirectory(String prefix) {
        try {
            logger.info("Listando archivos en directorio: {}", prefix);
            List<S3Object> files = s3Service.listFiles(prefix);
            logger.info("Se encontraron {} archivos", files.size());
            return files;
            } catch (Exception e) {
            logger.error("Error al listar archivos: {}", e.getMessage(), e);
            throw new RuntimeException("Error al listar archivos de S3", e);
        }
    }

    public DivisionResult processDivision(String fileKey) {
        long startTime = System.currentTimeMillis();
        DivisionResult result = new DivisionResult();
        result.setOriginalFileKey(fileKey);
        result.setExpectedRecords(expectedRecords);
        result.setRecordsPerChunk(recordsPerChunk);
        
        try {
            logger.info("Iniciando procesamiento de división para archivo: {}", fileKey);
            logger.info("Configuración - Registros esperados: {}, Registros por chunk: {}", expectedRecords, recordsPerChunk);
            
            // 1. Obtener el archivo de S3
            String content = getFileFromS3(fileKey);
            logger.info("Archivo obtenido de S3, tamaño: {} bytes", content.length());
            
            // 2. Leer y contar registros
            List<String> lines = readLines(content);
            String header = lines.isEmpty() ? "" : lines.get(0);
            List<String> dataLines = lines.size() > 1 ? lines.subList(1, lines.size()) : new ArrayList<>();
            
            long totalRecords = dataLines.size();
            result.setTotalRecords(totalRecords);
            
            logger.info("Total de registros encontrados: {} (esperados: {})", totalRecords, expectedRecords);
            
            // 3. Validar cantidad de registros
            if (totalRecords != expectedRecords) {
                result.setSuccess(false);
                result.setValidationPassed(false);
                result.setMessage(String.format("Validación fallida: Se encontraron %d registros, se esperaban %d", 
                    totalRecords, expectedRecords));
                logger.warn(result.getMessage());
                result.setProcessingTimeMs(System.currentTimeMillis() - startTime);
                return result;
            }
            
            result.setValidationPassed(true);
            logger.info("Validación exitosa: El archivo contiene {} registros", totalRecords);
            
            // 4. Generar UUID para carpeta de salida
            String uuid = UUID.randomUUID().toString();
            String outputFolder = outputPrefix + uuid + "/";
            result.setOutputFolder(outputFolder);
            
            logger.info("Carpeta de salida generada: {}", outputFolder);
            
            // 5. Dividir el archivo en chunks
            int totalChunks = (int) Math.ceil((double) totalRecords / recordsPerChunk);
            result.setTotalChunks(totalChunks);
            
            logger.info("Dividiendo en {} archivos de {} registros cada uno", totalChunks, recordsPerChunk);
            
            Map<String, String> filesToUpload = new LinkedHashMap<>();
            
            for (int i = 0; i < totalChunks; i++) {
                int startIndex = i * recordsPerChunk;
                int endIndex = Math.min(startIndex + recordsPerChunk, (int) totalRecords);
                
                // Crear contenido del chunk con header
                StringBuilder chunkContent = new StringBuilder();
                chunkContent.append(header).append("\n");
                
                for (int j = startIndex; j < endIndex; j++) {
                    chunkContent.append(dataLines.get(j));
                    if (j < endIndex - 1) {
                        chunkContent.append("\n");
                    }
                }
                
                // Generar nombre del archivo
                String chunkFileName = String.format("chunk_%04d.csv", i + 1);
                String chunkKey = outputFolder + chunkFileName;
                
                filesToUpload.put(chunkKey, chunkContent.toString());
                result.addGeneratedFile(chunkKey);
                
                // Generar URL de S3 para el archivo
                String s3Url = String.format("s3://%s/%s", bucketName, chunkKey);
                result.addGeneratedFileUrl(s3Url);
                
                logger.debug("Chunk {}/{} preparado: {} ({} registros)", 
                    i + 1, totalChunks, chunkFileName, endIndex - startIndex);
            }
            
            // 6. Subir archivos a S3
            logger.info("Subiendo {} archivos a S3...", filesToUpload.size());
            int uploadedCount = s3Service.putFiles(filesToUpload);
            
            if (uploadedCount == totalChunks) {
                result.setSuccess(true);
                result.setMessage(String.format("División completada exitosamente. %d archivos generados en %s", 
                    totalChunks, outputFolder));
                logger.info("División completada exitosamente");
            } else {
                result.setSuccess(false);
                result.setMessage(String.format("División parcialmente completada. %d/%d archivos subidos", 
                    uploadedCount, totalChunks));
                logger.warn("Solo se subieron {}/{} archivos", uploadedCount, totalChunks);
            }
            
            result.setProcessingTimeMs(System.currentTimeMillis() - startTime);
            logger.info("Procesamiento completado en {} ms", result.getProcessingTimeMs());
            
            return result;
            
        } catch (Exception e) {
            logger.error("Error en procesamiento de división: {}", e.getMessage(), e);
            result.setSuccess(false);
            result.setValidationPassed(false);
            result.setError(e.getMessage());
            result.setMessage("Error al procesar división: " + e.getMessage());
            result.setProcessingTimeMs(System.currentTimeMillis() - startTime);
            return result;
        }
    }

    /**
     * Lee las líneas de un contenido String
     * @param content Contenido a leer
     * @return Lista de líneas
     */
    private List<String> readLines(String content) {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new StringReader(content))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    lines.add(line);
                }
            }
        } catch (Exception e) {
            logger.error("Error al leer líneas: {}", e.getMessage(), e);
            throw new RuntimeException("Error al leer líneas del archivo", e);
        }
        return lines;
    }
}
