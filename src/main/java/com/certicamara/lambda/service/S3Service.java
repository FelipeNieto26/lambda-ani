package com.certicamara.lambda.service;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ApplicationScoped
public class S3Service {

    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);

    @Inject
    S3Client s3Client;

    @ConfigProperty(name = "aws.s3.bucket.name")
    String bucketName;

    @ConfigProperty(name = "aws.region")
    String region;

    public S3Service() {
    }

    public S3Service(String bucketName, String region) {
        this.bucketName = bucketName;
        this.region = region;
        this.s3Client = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        logger.info("S3Service inicializado manualmente - Bucket: {}, Region: {}", bucketName, region);
    }

    /**
     * Inicializa el S3Client si no fue inyectado (por ejemplo al correr localmente).
     */
    @PostConstruct
    public void init() {
        if (s3Client == null) {
            logger.info("Inicializando S3Client manualmente para entorno local...");
            s3Client = S3Client.builder()
                    .region(Region.of(region))
                    .credentialsProvider(ProfileCredentialsProvider.create("default"))
                    .build();
        } else {
            logger.info("S3Client inyectado correctamente.");
        }
    }

    public String getFileContent(String key) {
        try {
            logger.info("Obteniendo archivo de S3 - Bucket: {}, Key: {}", bucketName, key);
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(getObjectRequest);
            String content = new BufferedReader(new InputStreamReader(s3Object, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));

            logger.info("Archivo obtenido exitosamente. Tamaño: {} bytes", content.length());
            return content;

        } catch (Exception e) {
            logger.error("Error al obtener archivo de S3: {}", e.getMessage(), e);
            throw new RuntimeException("Error al obtener archivo de S3: " + e.getMessage(), e);
        }
    }

    public ResponseInputStream<GetObjectResponse> getFileStream(String key) {
        try {
            logger.info("Obteniendo stream de archivo de S3 - Bucket: {}, Key: {}", bucketName, key);
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            return s3Client.getObject(getObjectRequest);
        } catch (Exception e) {
            logger.error("Error al obtener stream de archivo de S3: {}", e.getMessage(), e);
            throw new RuntimeException("Error al obtener stream de archivo de S3: " + e.getMessage(), e);
        }
    }

    public List<S3Object> listFiles(String prefix) {
        try {
            logger.info("Listando archivos en S3 - Bucket: {}, Prefix: {}", bucketName, prefix);
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(prefix)
                    .build();

            ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
            List<S3Object> objects = listResponse.contents();
            logger.info("Se encontraron {} archivos", objects.size());
            return objects;
        } catch (Exception e) {
            logger.error("Error al listar archivos de S3: {}", e.getMessage(), e);
            throw new RuntimeException("Error al listar archivos de S3: " + e.getMessage(), e);
        }
    }

    public boolean fileExists(String key) {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            s3Client.getObject(getObjectRequest).close();
            return true;
        } catch (Exception e) {
            logger.debug("Archivo no existe o no es accesible: {}", key);
            return false;
        }
    }

    public void putFile(String key, String content) {
        try {
            logger.info("Subiendo archivo a S3 - Bucket: {}, Key: {}, Size: {} bytes", bucketName, key, content.length());
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType("text/csv")
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromString(content));
            logger.info("Archivo subido exitosamente a S3: {}", key);
        } catch (Exception e) {
            logger.error("Error al subir archivo a S3: {}", e.getMessage(), e);
            throw new RuntimeException("Error al subir archivo a S3: " + e.getMessage(), e);
        }
    }

    public int putFiles(Map<String, String> files) {
        int uploadedCount = 0;
        for (Map.Entry<String, String> entry : files.entrySet()) {
            try {
                putFile(entry.getKey(), entry.getValue());
                uploadedCount++;
            } catch (Exception e) {
                logger.error("Error al subir archivo {}: {}", entry.getKey(), e.getMessage());
            }
        }
        logger.info("Total de archivos subidos: {}/{}", uploadedCount, files.size());
        return uploadedCount;
    }
}
