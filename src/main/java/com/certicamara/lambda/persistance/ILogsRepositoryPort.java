package com.certicamara.lambda.domain.persistance;

import io.smallrye.mutiny.Uni;

/**
 * Puerto para actualizar logs en DynamoDB
 */
public interface ILogsRepositoryPort {
    
    /**
     * Actualiza el estado de un registro en DynamoDB
     * @param id UUID del registro
     * @param status Nuevo estado (EN PROGRESO, COMPLETADO, ERROR)
     * @return Uni que completa cuando la actualización termina
     */
    Uni<Void> updateStatusLog(String id, String status);
}

