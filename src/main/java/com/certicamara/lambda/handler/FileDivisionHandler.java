package com.certicamara.lambda.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.certicamara.lambda.model.DivisionResult;
import com.certicamara.lambda.service.FileDivisionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@Named("get")
@ApplicationScoped
public class FileDivisionHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final Logger logger = LoggerFactory.getLogger(FileDivisionHandler.class);

    @Inject
    FileDivisionService fileDivisionService;

    @ConfigProperty(name = "aws.s3.file.name")
    String fileName;

    @ConfigProperty(name = "aws.s3.default.prefix")
    String defaultPrefix;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        logger.info("Iniciando procesamiento de solicitud GET");
        
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setHeaders(Map.of(
            "Content-Type", "application/json",
            "Access-Control-Allow-Origin", "*"
        ));

        try {
            Map<String, String> queryParams = request.getQueryStringParameters();
            String prefix = (queryParams != null && queryParams.containsKey("prefix")) 
                ? queryParams.get("prefix") 
                : defaultPrefix;
            
            String fileKey = prefix + fileName;
            
            logger.info("Iniciando división de archivo: {} (fileName: {}, prefix: {})", fileKey, fileName, prefix);
            
            // Procesar división del archivo
            DivisionResult result = fileDivisionService.processDivision(fileKey);
            
            // Determinar código de estado HTTP
            int statusCode = result.isSuccess() ? 200 : 400;
            
            response.setStatusCode(statusCode);
            response.setBody(objectMapper.writeValueAsString(result));
            
            if (result.isSuccess()) {
                logger.info("División completada exitosamente - {} archivos generados en {} ms", 
                    result.getTotalChunks(), result.getProcessingTimeMs());
            } else {
                logger.warn("División fallida: {}", result.getMessage());
            }
            
            return response;
            
        } catch (Exception e) {
            logger.error("Error procesando solicitud: {}", e.getMessage(), e);
            return createErrorResponse(500, "Error interno del servidor: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent createErrorResponse(int statusCode, String message) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(statusCode);
        response.setHeaders(Map.of(
            "Content-Type", "application/json",
            "Access-Control-Allow-Origin", "*"
        ));
        
        try {
            Map<String, Object> errorBody = new HashMap<>();
            errorBody.put("success", false);
            errorBody.put("error", message);
            response.setBody(objectMapper.writeValueAsString(errorBody));
        } catch (Exception e) {
            response.setBody("{\"success\":false,\"error\":\"" + message + "\"}");
        }
        
        return response;
    }
}
