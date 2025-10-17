package com.certicamara.lambda.model;

import java.util.ArrayList;
import java.util.List;

public class DivisionResult {
    private boolean success;
    private String message;
    private String originalFileKey;
    private long totalRecords;
    private int expectedRecords;
    private boolean validationPassed;
    private String outputFolder;
    private int totalChunks;
    private int recordsPerChunk;
    private List<String> generatedFiles;
    private List<String> generatedFilesUrls;
    private long processingTimeMs;
    private String error;

    public DivisionResult() {
        this.generatedFiles = new ArrayList<>();
        this.generatedFilesUrls = new ArrayList<>();
    }

    // Getters y Setters
    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getOriginalFileKey() {
        return originalFileKey;
    }

    public void setOriginalFileKey(String originalFileKey) {
        this.originalFileKey = originalFileKey;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(long totalRecords) {
        this.totalRecords = totalRecords;
    }

    public int getExpectedRecords() {
        return expectedRecords;
    }

    public void setExpectedRecords(int expectedRecords) {
        this.expectedRecords = expectedRecords;
    }

    public boolean isValidationPassed() {
        return validationPassed;
    }

    public void setValidationPassed(boolean validationPassed) {
        this.validationPassed = validationPassed;
    }

    public String getOutputFolder() {
        return outputFolder;
    }

    public void setOutputFolder(String outputFolder) {
        this.outputFolder = outputFolder;
    }

    public int getTotalChunks() {
        return totalChunks;
    }

    public void setTotalChunks(int totalChunks) {
        this.totalChunks = totalChunks;
    }

    public int getRecordsPerChunk() {
        return recordsPerChunk;
    }

    public void setRecordsPerChunk(int recordsPerChunk) {
        this.recordsPerChunk = recordsPerChunk;
    }

    public List<String> getGeneratedFiles() {
        return generatedFiles;
    }

    public void setGeneratedFiles(List<String> generatedFiles) {
        this.generatedFiles = generatedFiles;
    }

    public void addGeneratedFile(String file) {
        this.generatedFiles.add(file);
    }

    public List<String> getGeneratedFilesUrls() {
        return generatedFilesUrls;
    }

    public void setGeneratedFilesUrls(List<String> generatedFilesUrls) {
        this.generatedFilesUrls = generatedFilesUrls;
    }

    public void addGeneratedFileUrl(String fileUrl) {
        this.generatedFilesUrls.add(fileUrl);
    }

    public long getProcessingTimeMs() {
        return processingTimeMs;
    }

    public void setProcessingTimeMs(long processingTimeMs) {
        this.processingTimeMs = processingTimeMs;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}


