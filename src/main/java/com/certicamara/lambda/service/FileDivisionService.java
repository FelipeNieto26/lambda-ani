package com.certicamara.lambda.service;

import com.certicamara.lambda.model.DivisionResult;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler.SheetContentsHandler;
import org.apache.poi.xssf.model.SharedStrings;
import org.apache.poi.xssf.model.StylesTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class FileDivisionService {

    private static final Logger logger = LoggerFactory.getLogger(FileDivisionService.class);
    private static final PrintStream out = System.out;

    private S3Service s3Service;
    private SQSService sqsService;
    private int expectedRecords;
    private int recordsPerChunk;
    private String outputPrefix;
    private String bucketName;
    private String region;
    private String currentIdLog; // Para pasar idLog a las clases internas

    public FileDivisionService() {
    }
    public FileDivisionService(
            S3Service s3Service,
            int expectedRecords,
            int recordsPerChunk,
            String outputPrefix,
            String bucketName,
            String region) {
        this.s3Service = s3Service;
        this.expectedRecords = expectedRecords;
        this.recordsPerChunk = recordsPerChunk;
        this.outputPrefix = outputPrefix;
        this.bucketName = bucketName;
        this.region = region;

        logger.info("FileDivisionService inicializado - expectedRecords: {}, recordsPerChunk: {}",
            expectedRecords, recordsPerChunk);
    }

    public FileDivisionService(
            S3Service s3Service,
            SQSService sqsService,
            int expectedRecords,
            int recordsPerChunk,
            String outputPrefix,
            String bucketName,
            String region) {
        this.s3Service = s3Service;
        this.sqsService = sqsService;
        this.expectedRecords = expectedRecords;
        this.recordsPerChunk = recordsPerChunk;
        this.outputPrefix = outputPrefix;
        this.bucketName = bucketName;
        this.region = region;

        logger.info("FileDivisionService inicializado con SQS - expectedRecords: {}, recordsPerChunk: {}",
            expectedRecords, recordsPerChunk);
    }


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
            logger.info("=== INICIANDO PROCESAMIENTO DE DIVISIÓN ===");
            logger.info("Archivo: {}", fileKey);
            logger.info("Configuración - Registros esperados: {}, Registros por chunk: {}", expectedRecords, recordsPerChunk);

            // 4. Generar UUID para carpeta de salida primero
            String uuid = UUID.randomUUID().toString();
            String outputFolder = outputPrefix + uuid + "/";
            result.setOutputFolder(outputFolder);
            logger.info("Carpeta de salida: {}", outputFolder);

            // Procesar el archivo directamente desde el stream
            long totalRecords = processFileStreamOptimized(fileKey, outputFolder, result, currentIdLog);
            result.setTotalRecords(totalRecords);

            logger.info("Total de registros procesados: {} (NO se valida cantidad exacta, se aceptan todos)", totalRecords);

            // 3. NO validar cantidad exacta - se procesan todos los registros que vengan
            logger.info("Validación deshabilitada: Se procesarán todos los registros sin validar cantidad exacta");

            result.setValidationPassed(true);
            result.setSuccess(true);
            result.setMessage(String.format("División completada exitosamente. %d archivos generados en %s",
                result.getTotalChunks(), outputFolder));

            result.setProcessingTimeMs(System.currentTimeMillis() - startTime);
            logger.info("=== PROCESAMIENTO COMPLETADO EN {} ms ===", result.getProcessingTimeMs());

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
     * Procesa la división de un archivo con carpeta de salida personalizada
     * @param fileKey Ruta del archivo en S3 (ej: "uuid123/archivo.csv")
     * @param outputFolder Carpeta de salida personalizada (ej: "uuid123/output/")
     * @return Resultado de la división
     */
    public DivisionResult processDivisionWithCustomOutput(String fileKey, String outputFolder, String idLog) {
        out.println("[INFO] ===== processDivisionWithCustomOutput INICIADO =====");
        out.println("[INFO] fileKey: " + fileKey);
        out.println("[INFO] outputFolder: " + outputFolder);
        out.println("[INFO] idLog: " + idLog);

        long startTime = System.currentTimeMillis();
        DivisionResult result = new DivisionResult();
        result.setOriginalFileKey(fileKey);
        result.setExpectedRecords(expectedRecords);
        result.setRecordsPerChunk(recordsPerChunk);
        result.setOutputFolder(outputFolder);

        // Extraer UUID del outputFolder (formato: "uuid123/output/")
        String uuid = extractUuidFromOutputFolder(outputFolder);

        out.println("[INFO] UUID extraído: " + uuid);
        out.println("[INFO] recordsPerChunk: " + recordsPerChunk);
        out.println("[INFO] expectedRecords: " + expectedRecords);

        // Establecer el idLog actual para uso en las clases internas
        this.currentIdLog = idLog;

        try {
            out.println("[INFO] === INICIANDO PROCESAMIENTO DE DIVISIÓN (CUSTOM OUTPUT) ===");
            out.println("[INFO] Archivo de entrada: " + fileKey);
            out.println("[INFO] Carpeta de salida: " + outputFolder);
            out.println("[INFO] UUID extraído: " + uuid);
            out.println("[INFO] Configuración - Registros esperados: " + expectedRecords + ", Registros por chunk: " + recordsPerChunk);

            logger.info("=== INICIANDO PROCESAMIENTO DE DIVISIÓN (CUSTOM OUTPUT) ===");
            logger.info("Archivo de entrada: {}", fileKey);
            logger.info("Carpeta de salida: {}", outputFolder);
            logger.info("UUID extraído: {}", uuid);
            logger.info("Configuración - Registros esperados: {}, Registros por chunk: {}", expectedRecords, recordsPerChunk);

            // Procesar el archivo directamente desde el stream
            long totalRecords = processFileStreamOptimized(fileKey, outputFolder, result, idLog);
            result.setTotalRecords(totalRecords);

            logger.info("Total de registros procesados: {} (NO se valida cantidad exacta, se aceptan todos)", totalRecords);

            // NO validar cantidad exacta - se procesan todos los registros que vengan
            logger.info("Validación deshabilitada: Se procesarán todos los registros sin validar cantidad exacta");

            result.setValidationPassed(true);
            result.setSuccess(true);
            result.setMessage(String.format("División completada exitosamente. %d archivos generados en %s",
                result.getTotalChunks(), outputFolder));

            result.setProcessingTimeMs(System.currentTimeMillis() - startTime);
            logger.info("=== PROCESAMIENTO COMPLETADO EN {} ms ===", result.getProcessingTimeMs());

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
     * Extrae el UUID del outputFolder
     * @param outputFolder Formato esperado: "uuid123/output/" o "uuid123/output"
     * @return El UUID extraído
     */
    private String extractUuidFromOutputFolder(String outputFolder) {
        if (outputFolder == null || outputFolder.isEmpty()) {
            return "unknown";
        }

        // Remover trailing slash si existe
        String folder = outputFolder.endsWith("/") ? outputFolder.substring(0, outputFolder.length() - 1) : outputFolder;

        // Dividir por / y tomar la primera parte (el UUID)
        String[] parts = folder.split("/");
        if (parts.length > 0) {
            return parts[0];
        }

        return "unknown";
    }

    /**
     * Valida que la extensión del archivo sea xlsx o csv
     */
    private void validateFileExtension(String fileKey) {
        String lowerCaseKey = fileKey.toLowerCase();
        if (!lowerCaseKey.endsWith(".csv") && !lowerCaseKey.endsWith(".xlsx")) {
            throw new IllegalArgumentException(
                "Tipo de archivo no soportado. Solo se permiten archivos .csv o .xlsx. Archivo recibido: " + fileKey
            );
        }
        logger.info("Validación de extensión exitosa: {}", lowerCaseKey.endsWith(".csv") ? "CSV" : "XLSX");
    }

    /**
     * Detecta si el archivo es XLSX basándose en la extensión
     */
    private boolean isExcelFile(String fileKey) {
        return fileKey.toLowerCase().endsWith(".xlsx");
    }

    /**
     * MÉTODO ULTRA-OPTIMIZADO: Convierte XLSX a chunks directamente SIN archivo CSV intermedio
     * Procesamiento por lotes - convierte y sube chunks mientras lee el XLSX
     */
    private long convertXlsxAndProcessDirectly(String xlsxKey, InputStream inputStream, String outputFolder, DivisionResult result) throws Exception {
        out.println("[INFO] ===== convertXlsxAndProcessDirectly INICIADO =====");
        out.println("[INFO] xlsxKey: " + xlsxKey);
        out.println("[INFO] outputFolder: " + outputFolder);
        out.println("[INFO] === PROCESAMIENTO XLSX POR LOTES (SIN CSV INTERMEDIO) ===");
        logger.info("=== PROCESAMIENTO XLSX POR LOTES (SIN CSV INTERMEDIO) ===");
        long startTime = System.currentTimeMillis();

        java.io.File tempXlsx = java.io.File.createTempFile("xlsx-input", ".xlsx");

        try {
            // 1. Descargar XLSX
            logger.info("📥 Paso 1/2: Descargando XLSX...");
            System.out.println("Descargando XLSX...");
            long downloadStart = System.currentTimeMillis();

            try (java.io.FileOutputStream fos = new java.io.FileOutputStream(tempXlsx);
                 java.io.BufferedOutputStream bos = new java.io.BufferedOutputStream(fos, 131072)) {
                byte[] buffer = new byte[131072];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    bos.write(buffer, 0, bytesRead);
                }
            }
            logger.info("   ✅ Descargado: {} MB en {} ms",
                    tempXlsx.length() / (1024 * 1024), System.currentTimeMillis() - downloadStart);

            // 2. Procesar XLSX directamente a chunks por lotes
            out.println("[INFO] 🔄 Paso 2/2: Procesando XLSX por lotes → Chunks directos...");
            out.println("[INFO] Estrategia: Leer " + recordsPerChunk + " filas → Convertir a CSV → Subir chunk → Repetir");
            logger.info("🔄 Paso 2/2: Procesando XLSX por lotes → Chunks directos...");
            logger.info("   Estrategia: Leer {} filas → Convertir a CSV → Subir chunk → Repetir", recordsPerChunk);
            long processingStart = System.currentTimeMillis();

            long totalRecords = processXlsxInBatches(tempXlsx, outputFolder, result);

            out.println("[INFO] ✅ Procesamiento por lotes: " + (System.currentTimeMillis() - processingStart) + " ms");
            logger.info("   ✅ Procesamiento por lotes: {} ms", System.currentTimeMillis() - processingStart);

            long totalTime = System.currentTimeMillis() - startTime;
            out.println("[INFO] === PROCESAMIENTO XLSX COMPLETADO ===");
            out.println("[INFO] ✅ Total registros: " + totalRecords);
            out.println("[INFO] ✅ Total chunks: " + result.getTotalChunks());
            out.println("[INFO] ✅ Tiempo total: " + totalTime + " ms (" + (totalTime / 1000) + " segundos)");
            logger.info("=== PROCESAMIENTO XLSX COMPLETADO ===");
            logger.info("✅ Total registros: {}", totalRecords);
            logger.info("✅ Total chunks: {}", result.getTotalChunks());
            logger.info("✅ Tiempo total: {} ms ({} segundos)", totalTime, totalTime / 1000);
            logger.info("💾 Memoria: Procesamiento por lotes - sin CSV intermedio");

            return totalRecords;

        } finally {
            if (tempXlsx.exists()) tempXlsx.delete();
        }
    }

    /**
     * Procesa XLSX por lotes directamente a chunks
     * Lee N filas → Convierte a CSV → Sube chunk → Repite
     */
    private long processXlsxInBatches(java.io.File xlsxFile, String outputFolder, DivisionResult result) throws Exception {
        out.println("[INFO] processXlsxInBatches INICIADO");
        out.println("[INFO] xlsxFile.length(): " + xlsxFile.length() + " bytes");
        out.println("[INFO] outputFolder: " + outputFolder);

        DirectBatchXlsxProcessor processor = new DirectBatchXlsxProcessor(outputFolder, result);

        try (OPCPackage pkg = OPCPackage.open(xlsxFile)) {
            out.println("[INFO] OPCPackage abierto");
            XSSFReader reader = new XSSFReader(pkg);
            SharedStrings sst = reader.getSharedStringsTable();
            StylesTable styles = reader.getStylesTable();
            out.println("[INFO] XSSFReader y tablas compartidas obtenidas");

            XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
            int sheetIdx = 0;

            while (sheets.hasNext()) {
                try (InputStream sheetStream = sheets.next()) {
                    sheetIdx++;
                    String sheetName = sheets.getSheetName();
                    out.println("[INFO] Procesando hoja #" + sheetIdx + " (" + sheetName + ")");

                    // *** FIX 1: el parser debe ser namespace-aware ***
                    SAXParserFactory saxFactory = SAXParserFactory.newInstance();
                    saxFactory.setNamespaceAware(true);

                    SAXParser saxParser = saxFactory.newSAXParser();
                    XMLReader xmlReader = saxParser.getXMLReader();

                    // *** FIX 2: usar DataFormatter para valores tal como Excel los muestra ***
                    DataFormatter formatter = new DataFormatter();

                    // *** FIX 3: constructor recomendado con comments=null y formatter ***
                    ContentHandler contentHandler = new XSSFSheetXMLHandler(
                            styles,        // StylesTable
                            /*comments*/ null,
                            sst,           // SharedStrings
                            processor,     // SheetContentsHandler
                            formatter,     // DataFormatter
                            /*formulasNotResults*/ false
                    );

                    out.println("[INFO] Iniciando parseo XML de hoja: " + sheetName);
                    xmlReader.setContentHandler(contentHandler);
                    xmlReader.parse(new InputSource(sheetStream));
                    out.println("[INFO] Parseo XML completado para hoja: " + sheetName);
                }
            }

            if (sheetIdx == 0) {
                out.println("[INFO] NO HAY HOJAS EN EL ARCHIVO");
            }
        }

        out.println("Saliendo de procesar XML");
        out.println("[INFO] Total records (datos): " + processor.getTotalRecords());
        return processor.getTotalRecords();
    }

    /**
     * Procesador que convierte y sube chunks por lotes mientras lee XLSX
     * NO acumula más de recordsPerChunk filas en memoria
     */
    private class DirectBatchXlsxProcessor implements SheetContentsHandler {
        private final String outputFolder;
        private final DivisionResult result;
        private final List<String> currentRow = new ArrayList<>();
        private String header = null;
        private long totalRecords = 0;            // solo filas de datos
        private long rowsSeen = 0;                // incluye header
        private int currentChunkIndex = 0;
        private int recordsInCurrentBatch = 0;
        private StringBuilder batchContent = new StringBuilder();

        public DirectBatchXlsxProcessor(String outputFolder, DivisionResult result) {
            this.outputFolder = outputFolder;
            this.result = result;
            out.println("[INFO] DirectBatchXlsxProcessor inicializado");
        }

        @Override
        public void startRow(int rowNum) {
            currentRow.clear();
            if (rowNum <= 5) {
                out.println("[DEBUG] DirectBatchXlsxProcessor.startRow - rowNum: " + rowNum);
            }
        }

        @Override
        public void endRow(int rowNum) {
            rowsSeen++;
            if (rowNum <= 5) {
                out.println("[DEBUG] DirectBatchXlsxProcessor.endRow - rowNum: " + rowNum + ", currentRow.size(): " + currentRow.size());
            }
            if (currentRow.isEmpty()) {
                if (rowNum <= 5) {
                    out.println("[DEBUG] DirectBatchXlsxProcessor.endRow - currentRow VACÍO para rowNum: " + rowNum);
                }
                return;
            }

            // Convertir fila XLSX a CSV con ';'
            StringBuilder rowCsv = new StringBuilder();
            for (int i = 0; i < currentRow.size(); i++) {
                String value = currentRow.get(i);
                if (value.contains(";") || value.contains("\"") || value.contains("\n")) {
                    value = "\"" + value.replace("\"", "\"\"") + "\"";
                }
                rowCsv.append(value);
                if (i < currentRow.size() - 1) {
                    rowCsv.append(";");
                }
            }

            if (header == null) {
                header = rowCsv.toString();
                batchContent.append(header).append("\n");
                out.println("[INFO] Header detectado con " + currentRow.size() + " columnas");
                logger.info("   Header: {} columnas", currentRow.size());
                return;
            }

            // agregar fila de datos al lote actual
            batchContent.append(rowCsv).append("\n");
            recordsInCurrentBatch++;
            totalRecords++;

            if (totalRecords % 50000 == 0) {
                out.println("[INFO] 📊 " + totalRecords + " registros procesados, " + currentChunkIndex + " chunks generados");
                logger.info("   📊 {} registros procesados, {} chunks generados", totalRecords, currentChunkIndex);
            }

            if (recordsInCurrentBatch >= recordsPerChunk) {
                out.println("[INFO] Lote alcanzó recordsPerChunk (" + recordsPerChunk + "), llamando uploadBatch()");
                uploadBatch();
            }
        }

        private void uploadBatch() {
            out.println("[DEBUG] uploadBatch() llamado - recordsInCurrentBatch: " + recordsInCurrentBatch + ", batchContent.length(): " + batchContent.length());
            if (recordsInCurrentBatch == 0) {
                out.println("[DEBUG] No hay registros para subir, retornando");
                return;
            }

            try {
                out.println("[INFO] Subiendo chunk " + currentChunkIndex + " con " + recordsInCurrentBatch + " registros");
                long uploadStart = System.currentTimeMillis();
                uploadChunk(outputFolder, currentChunkIndex, batchContent.toString(), result, currentIdLog);
                long uploadTime = System.currentTimeMillis() - uploadStart;

                currentChunkIndex++;
                out.println("[INFO] ✅ Chunk " + currentChunkIndex + " subido: " + recordsInCurrentBatch + " registros (" + uploadTime + " ms)");
                logger.debug("   ✅ Chunk {} subido: {} registros ({} ms)",
                        currentChunkIndex, recordsInCurrentBatch, uploadTime);

                // reset del buffer
                recordsInCurrentBatch = 0;
                batchContent = new StringBuilder();
                batchContent.append(header).append("\n");
            } catch (Exception e) {
                out.println("[ERROR] ❌ Error al subir chunk " + currentChunkIndex + ": " + e.getMessage());
                e.printStackTrace();
                logger.error("❌ Error al subir chunk {}: {}", currentChunkIndex, e.getMessage());
                throw new RuntimeException("Error al subir chunk", e);
            }
        }

        @Override
        public void cell(String cellReference, String formattedValue, org.apache.poi.xssf.usermodel.XSSFComment comment) {
            currentRow.add(formattedValue != null ? formattedValue : "");
            if (currentRow.size() <= 3) {
//                out.println("[DEBUG] DirectBatchXlsxProcessor.cell - cellReference: " + cellReference +
//                        ", formattedValue: " + formattedValue + ", currentRow.size(): " + currentRow.size());
            }
        }

        @Override
        public void headerFooter(String text, boolean isHeader, String tagName) {
            // No usado
        }

        @Override
        public void endSheet() {
            out.println("[INFO] endSheet() llamado - totalRecords: " + totalRecords + ", recordsInCurrentBatch: " + recordsInCurrentBatch + ", currentChunkIndex: " + currentChunkIndex);
            uploadBatch(); // subir lo que quede
            result.setTotalChunks(currentChunkIndex);
            out.println("[INFO] ✅ Total filas (incluye header): " + rowsSeen);
            out.println("[INFO] ✅ Total filas de datos: " + totalRecords);
            logger.info("   ✅ Total filas (incluye header): {}", rowsSeen);
            logger.info("   ✅ Total filas de datos: {}", totalRecords);
            logger.info("   ✅ Total chunks generados: {}", currentChunkIndex);
        }

        public long getTotalRecords() {
            return totalRecords;
        }
    }
    /**
     * CLASE DEPRECADA - Se mantiene por si se necesita en el futuro
     * Procesador que convierte XLSX a CSV y genera chunks DIRECTAMENTE
     * Sin paso intermedio de guardar CSV completo
     */
    @Deprecated
    private class DirectXlsxToChunksProcessor implements SheetContentsHandler {
        private final String outputFolder;
        private final DivisionResult result;
        private final List<String> currentRow = new ArrayList<>();
        private String header = null;
        private long totalRecords = 0;
        private int currentChunkIndex = 0;
        private int recordsInCurrentChunk = 0;
        private StringBuilder chunkContent = new StringBuilder();

        public DirectXlsxToChunksProcessor(String outputFolder, DivisionResult result) {
            this.outputFolder = outputFolder;
            this.result = result;
        }

        @Override
        public void startRow(int rowNum) {
            currentRow.clear();
        }

        @Override
        public void endRow(int rowNum) {
            if (currentRow.isEmpty()) {
                return;
            }

            // Convertir fila a CSV con separador ;
            StringBuilder rowCsv = new StringBuilder();
            for (int i = 0; i < currentRow.size(); i++) {
                String value = currentRow.get(i);

                // Escapar valores con ; o comillas
                if (value.contains(";") || value.contains("\"") || value.contains("\n")) {
                    value = "\"" + value.replace("\"", "\"\"") + "\"";
                }

                rowCsv.append(value);
                if (i < currentRow.size() - 1) {
                    rowCsv.append(";");
                }
            }

            if (header == null) {
                // Primera fila es el header
                header = rowCsv.toString();
                chunkContent.append(header).append("\n");
                logger.info("   Header: {} columnas", currentRow.size());
            } else {
                // Filas de datos - agregar al chunk actual
                chunkContent.append(rowCsv).append("\n");
                recordsInCurrentChunk++;
                totalRecords++;

                // Log cada 50,000 registros
                if (totalRecords % 50000 == 0) {
                    logger.info("   📊 {} registros procesados, {} chunks generados",
                            totalRecords, currentChunkIndex);
                }

                // Subir chunk cuando alcanza el límite
                if (recordsInCurrentChunk >= recordsPerChunk) {
                    try {
                        long uploadStart = System.currentTimeMillis();
                        uploadChunk(outputFolder, currentChunkIndex, chunkContent.toString(), result, currentIdLog);
                        long uploadTime = System.currentTimeMillis() - uploadStart;

                        currentChunkIndex++;
                        logger.debug("   ✅ Chunk {} subido: {} registros ({} ms)",
                                currentChunkIndex, recordsInCurrentChunk, uploadTime);

                        recordsInCurrentChunk = 0;
                        chunkContent = new StringBuilder();
                        chunkContent.append(header).append("\n");
                    } catch (Exception e) {
                        logger.error("❌ Error al subir chunk {}: {}", currentChunkIndex, e.getMessage());
                        throw new RuntimeException("Error al subir chunk", e);
                    }
                }
            }
        }

        @Override
        public void cell(String cellReference, String formattedValue,
                        org.apache.poi.xssf.usermodel.XSSFComment comment) {
            currentRow.add(formattedValue != null ? formattedValue : "");
        }

        @Override
        public void headerFooter(String text, boolean isHeader, String tagName) {
            // No usado
        }

        @Override
        public void endSheet() {
            // Subir último chunk si tiene contenido
            if (recordsInCurrentChunk > 0) {
                try {
                    uploadChunk(outputFolder, currentChunkIndex, chunkContent.toString(), result, currentIdLog);
                    logger.info("   ✅ Chunk final {} subido: {} registros",
                            currentChunkIndex + 1, recordsInCurrentChunk);
                    currentChunkIndex++;
                } catch (Exception e) {
                    throw new RuntimeException("Error al subir chunk final", e);
                }
            }
        }

        public long getTotalRecords() {
            return totalRecords;
        }

        public int getCurrentChunkIndex() {
            return currentChunkIndex;
        }
    }

    /**
     * Convierte una fila de Excel a formato CSV string (usado por métodos legacy)
     */
    private String rowToCsvString(Row row, int numberOfColumns) {
        StringBuilder rowContent = new StringBuilder();

        for (int cellIndex = 0; cellIndex < numberOfColumns; cellIndex++) {
            Cell cell = row.getCell(cellIndex);
            String cellValue = getCellValueAsString(cell);

            // Escapar comillas y encerrar si contiene caracteres especiales
            if (cellValue.contains(",") || cellValue.contains("\"") || cellValue.contains("\n")) {
                cellValue = "\"" + cellValue.replace("\"", "\"\"") + "\"";
            }

            rowContent.append(cellValue);
            if (cellIndex < numberOfColumns - 1) {
                rowContent.append(",");
            }
        }

        return rowContent.toString();
    }

    /**
     * Verifica si una fila está completamente vacía
     */
    private boolean isRowEmpty(Row row) {
        if (row == null) {
            return true;
        }

        for (int cellIndex = row.getFirstCellNum(); cellIndex < row.getLastCellNum(); cellIndex++) {
            Cell cell = row.getCell(cellIndex);
            if (cell != null && cell.getCellType() != CellType.BLANK) {
                String cellValue = getCellValueAsString(cell);
                if (cellValue != null && !cellValue.trim().isEmpty()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Obtiene el valor de una celda como String
     */
    private String getCellValueAsString(Cell cell) {
        if (cell == null) {
            return "";
        }

        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toString();
                } else {
                    // Evitar notación científica para números
                    double numericValue = cell.getNumericCellValue();
                    if (numericValue == (long) numericValue) {
                        return String.valueOf((long) numericValue);
                    } else {
                        return String.valueOf(numericValue);
                    }
                }
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                try {
                    return cell.getStringCellValue();
                } catch (Exception e) {
                    return String.valueOf(cell.getNumericCellValue());
                }
            case BLANK:
                return "";
            default:
                return "";
        }
    }

    /**
     * Procesa el archivo de forma optimizada para memoria usando streaming
     */
    private long processFileStreamOptimized(String fileKey, String outputFolder, DivisionResult result, String idLog) {
        out.println("[INFO] ===== processFileStreamOptimized INICIADO =====");
        out.println("[INFO] Bucket: " + bucketName);
        out.println("[INFO] Key (ruta completa): " + fileKey);
        out.println("[INFO] Region: " + region);
        out.println("[INFO] Ruta S3 completa: s3://" + bucketName + "/" + fileKey);
        out.println("[INFO] idLog: " + idLog);

        try {
            out.println("[INFO] === INTENTANDO OBTENER ARCHIVO DE S3 ===");
            logger.info("=== INTENTANDO OBTENER ARCHIVO DE S3 ===");
            logger.info("Bucket: {}", bucketName);
            logger.info("Key (ruta completa): {}", fileKey);
            logger.info("Region: {}", region);
            logger.info("Ruta S3 completa: s3://{}/{}", bucketName, fileKey);

            // Validar extensión del archivo
            validateFileExtension(fileKey);

            out.println("[INFO] Descargando stream del archivo directamente...");
            logger.info("Descargando stream del archivo directamente...");
            var s3Stream = s3Service.getFileStream(fileKey);

            if (isExcelFile(fileKey)) {
                out.println("[INFO] ✅ Archivo XLSX detectado: " + fileKey);
                out.println("[INFO] ESTRATEGIA ULTRA-OPTIMIZADA: XLSX → CSV en memoria → Chunks directos (sin S3 intermedio)");
                logger.info("✅ Archivo XLSX detectado: {}", fileKey);
                logger.info("ESTRATEGIA ULTRA-OPTIMIZADA: XLSX → CSV en memoria → Chunks directos (sin S3 intermedio)");

                // Convertir XLSX a CSV en memoria y procesar DIRECTAMENTE sin guardar en S3
                return convertXlsxAndProcessDirectly(fileKey, s3Stream, outputFolder, result);
            } else {
                logger.info("✅ Archivo CSV detectado: {}", fileKey);
                logger.info("Se procesará preservando su formato original");
                return processCSVStream(s3Stream, outputFolder, result);
            }
        } catch (Exception e) {
            logger.error("=== ERROR AL PROCESAR ARCHIVO ===");
            logger.error("Tipo de error: {}", e.getClass().getName());
            logger.error("Mensaje de error: {}", e.getMessage());

            if (e.getMessage() != null && (e.getMessage().contains("NoSuchKey") ||
                e.getMessage().contains("does not exist") ||
                e.getMessage().contains("404"))) {

                logger.error("=== ARCHIVO NO ENCONTRADO EN S3 ===");
                logger.error("Intentando listar archivos en la carpeta para diagnosticar...");

                try {
                    String folderPath = fileKey.contains("/")
                        ? fileKey.substring(0, fileKey.lastIndexOf("/") + 1)
                        : "";

                    if (!folderPath.isEmpty()) {
                        logger.info("Listando archivos en: {}", folderPath);
                        List<S3Object> filesInFolder = s3Service.listFiles(folderPath);

                        if (filesInFolder.isEmpty()) {
                            logger.error("❌ La carpeta '{}' está VACÍA o no existe", folderPath);
                            logger.error("Verifica que:");
                            logger.error("1. El UUID '{}' es correcto", folderPath.replace("/", ""));
                            logger.error("2. El archivo fue subido a s3://{}/{}", bucketName, folderPath);
                        } else {
                            logger.info("✅ Archivos encontrados en '{}' ({} archivos):", folderPath, filesInFolder.size());
                            for (S3Object obj : filesInFolder) {
                                logger.info("  📄 {} ({} bytes)", obj.key(), obj.size());
                            }
                            logger.error("❌ El archivo '{}' NO está en esta lista", fileKey);
                            logger.error("Verifica que el nombre del archivo coincida EXACTAMENTE (case-sensitive)");
                        }
                    }
                } catch (Exception listException) {
                    logger.error("No se pudo listar archivos: {}", listException.getMessage());
                }
            }

            throw new RuntimeException("Error al procesar archivo: " + e.getMessage(), e);
        }
    }

    /**
     * Procesa contenido CSV desde un Stream (usado para archivos CSV directos)
     */
    private long processCSVStream(InputStream inputStream, String outputFolder, DivisionResult result) throws Exception {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, java.nio.charset.StandardCharsets.UTF_8))) {
            return processCSVFromReader(reader, outputFolder, result);
        }
    }

    /**
     * Procesa el contenido CSV desde un BufferedReader
     * IMPORTANTE: Mantiene el separador original del CSV (no lo modifica)
     */
    private long processCSVFromReader(BufferedReader reader, String outputFolder, DivisionResult result) throws Exception {
        logger.info("=== INICIANDO PROCESAMIENTO CSV ===");
        logger.info("El CSV se procesa preservando su formato original (incluyendo separador)");

        String header = reader.readLine();
        if (header == null) {
            throw new RuntimeException("Archivo vacío");
        }

        // Detectar el separador usado
        String detectedSeparator = header.contains(";") ? "punto y coma (;)" :
                header.contains(",") ? "coma (,)" : "desconocido";
        logger.info("Header leído: {} caracteres, Separador detectado: {}", header.length(), detectedSeparator);
        logger.info("Preview del header: {}", header.length() > 100 ? header.substring(0, 100) + "..." : header);

        long totalRecords = 0;
        int currentChunkIndex = 0;
        int recordsInCurrentChunk = 0;
        int emptyLinesSkipped = 0;
        StringBuilder chunkContent = new StringBuilder();
        chunkContent.append(header).append("\n");

        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) {
                emptyLinesSkipped++;
                continue;
            }

            chunkContent.append(line).append("\n");
            recordsInCurrentChunk++;
            totalRecords++;

            if (recordsInCurrentChunk >= recordsPerChunk) {
                long uploadStart = System.currentTimeMillis();
                uploadChunk(outputFolder, currentChunkIndex, chunkContent.toString(), result, currentIdLog);
                long uploadTime = System.currentTimeMillis() - uploadStart;

                currentChunkIndex++;
                logger.info("✅ Chunk {} subido: {} registros ({} ms)",
                        currentChunkIndex, recordsInCurrentChunk, uploadTime);

                recordsInCurrentChunk = 0;
                chunkContent = new StringBuilder();
                chunkContent.append(header).append("\n");

                // Log de progreso cada 5 chunks
                if (currentChunkIndex % 5 == 0) {
                    logger.info("📊 Progreso CSV: {} registros procesados, {} chunks subidos",
                        totalRecords, currentChunkIndex);
                }
            }
        }

        // Subir el último chunk si tiene contenido
        if (recordsInCurrentChunk > 0) {
            uploadChunk(outputFolder, currentChunkIndex, chunkContent.toString(), result, currentIdLog);
            logger.info("✅ Chunk final {} subido: {} registros", currentChunkIndex + 1, recordsInCurrentChunk);
            currentChunkIndex++;
        }

        result.setTotalChunks(currentChunkIndex);

        logger.info("=== PROCESAMIENTO CSV COMPLETADO ===");
        logger.info("Total registros procesados: {}", totalRecords);
        logger.info("Líneas vacías saltadas: {}", emptyLinesSkipped);
        logger.info("Total de chunks generados: {}", currentChunkIndex);
        logger.info("Registros por chunk configurado: {}", recordsPerChunk);

        return totalRecords;
    }

    /**
     * Sube un chunk a S3 (optimizado con retry automático)
     */
    private void uploadChunk(String outputFolder, int chunkIndex, String content, DivisionResult result, String idLog) {
            String chunkFileName = String.format("chunk_%04d.csv", chunkIndex + 1);
            String chunkKey = outputFolder + chunkFileName;

        int maxRetries = 3;
        int attempt = 0;
        Exception lastException = null;

        while (attempt < maxRetries) {
            try {
            s3Service.putFile(chunkKey, content);

            result.addGeneratedFile(chunkKey);
            String s3Url = String.format("s3://%s/%s", bucketName, chunkKey);
            result.addGeneratedFileUrl(s3Url);

            logger.debug("Chunk subido: {} ({} bytes)", chunkFileName, content.length());

            // Enviar mensaje SQS si se proporcionó idLog y sqsService está disponible
            if (idLog != null && sqsService != null) {
                try {
                    String outputBucket = "bucket-csv-divididos-ani"; // Bucket de destino
                    int sizeBatch = 100; // Siempre 100 según requerimientos
                    
                    logger.info("Enviando mensaje SQS para chunk: bucket={}, key={}, sizeBatch={}, idLog={}", 
                        outputBucket, chunkKey, sizeBatch, idLog);
                    
                    sqsService.sendChunkMessage(outputBucket, chunkKey, sizeBatch, idLog);
                    
                    logger.info("✅ Mensaje SQS enviado exitosamente para chunk {}", chunkFileName);
                    
                } catch (Exception sqsError) {
                    logger.error("❌ Error al enviar mensaje SQS para chunk {}: {}", chunkFileName, sqsError.getMessage());
                    // No fallar el procesamiento si SQS falla, solo loggear el error
                }
            } else {
                logger.debug("No se envió mensaje SQS - idLog: {}, sqsService: {}", idLog, (sqsService != null ? "disponible" : "null"));
            }

            return; // Éxito

        } catch (Exception e) {
                lastException = e;
                attempt++;
                if (attempt < maxRetries) {
                    logger.warn("Error al subir chunk {} (intento {}/{}): {}. Reintentando...",
                            chunkIndex, attempt, maxRetries, e.getMessage());
                    try {
                        Thread.sleep(100 * attempt); // Backoff exponencial
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        // Si llegamos aquí, todos los intentos fallaron
        logger.error("❌ Error al subir chunk {} después de {} intentos: {}",
                chunkIndex, maxRetries, lastException.getMessage());
        throw new RuntimeException("Error al subir chunk después de " + maxRetries + " intentos: "
                + lastException.getMessage(), lastException);
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
