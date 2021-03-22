package com.rafaat.image;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.List;

public class FileLogger {

    private String filename = "out.log";

    public FileLogger() {

    }

    public FileLogger(String filename) {
        this.setFilename(filename);
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getFilename() {
        return filename;
    }

    public FileLogger log(String str) {
        return appendToFile(getFilename(), LocalDateTime.now() + " - " + str);
    }

    public FileLogger recordSuccess(String dir) {
        return appendToFile("success.list", dir.split(":\\\\")[1]);
    }

    public FileLogger recordFailure(String dir) {
        return appendToFile("failed.list", dir.split(":\\\\")[1]);
    }

    public FileLogger appendToFile(String filename, String data) {
        try {
            FileUtils.writeStringToFile(new File(filename), data + "\n", Charset.defaultCharset(), true);
        } catch (IOException ioex) {
            throw new RuntimeException(ioex);
        }
        return this;
    }

    public boolean isSuccess(String dir) {
        dir = dir.split(":\\\\")[1];
        List<String> lines = readLinesFromFile("success.list");
        return lines.contains(dir);
    }

    public List<String> readLinesFromFile(String filename) {
        try {
            return FileUtils.readLines(new File(filename), Charset.defaultCharset());
        } catch (IOException ioex) {
            throw new RuntimeException(ioex);
        }
    }
}
