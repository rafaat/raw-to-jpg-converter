package com.rafaat.image;

import org.apache.commons.collections4.ListUtils;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;

public class ConvertRaw {

    private static final String photoRootDir = "E:\\Photos";
    private static final boolean deleteRawAfterConvert = false;
    private static final boolean deleteRawOnHighResJpg = false;
    private static final boolean deleteXmpFiles = false;
    private static final boolean dryRun = false;

    private static final String ufrawBatch = "C:\\Program Files (x86)\\UFRaw\\bin\\ufraw-batch.exe";
    private static final String opOutTypeJpg = "--out-type=jpg";
    private static final String opLensFun = "--lensfun=none";
    private static final String opQuality = "--compression=97";
    //private static final String opOutBit = "--out-depth=16";
    //private static final String opBaseCurve = "--base-curve-file=base.curve";
    private static final String opExposure = "--exposure=1.1";
    private static final String opSaturation = "--saturation=1.2";
    private static final String opRestoreDetails = "--restore=hsv";
    private static final String opClipFilm = "--clip=film";
    private static final String opAutoCrop = "--auto-crop";
    private static final String opNoOverwrite = "--overwrite";
    private static final String opColorSmoothing = "--color-smoothing";
    private static final String cr2 = ".cr2";
    private static final String nef = ".nef";
    private static final String xmp = ".xmp";
    private static final int lowResThreshold = 2000;

    private static final FileLogger fileLogger = new FileLogger("conversion.log");

    private static Map<String, List<File>> mapOfDirToFiles = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        fileLogger.log("").log("Starting...");
        convertDir(photoRootDir);
        fileLogger.log("Done!").log("");
        System.out.println("Done!");
        System.exit(0);
    }

    private static void convertDir(String dir) throws Exception {
        mapOfDirToFiles.clear();
        File folder = new File(dir);
        File[] files = folder.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                System.out.println(file.getAbsolutePath());
            }
        }

        findAndMatchFilesInDir(dir);

        ExecutorService parallelExecutor = Executors.newFixedThreadPool(2);
        List<Callable<Void>> ufrawTasks = new ArrayList<>();
        for (String subdir : mapOfDirToFiles.keySet()) {
            Callable<Void> c = () -> {
                convertFiles(mapOfDirToFiles.get(subdir), subdir);
                return null;
            };
            ufrawTasks.add(c);
        }
        parallelExecutor.invokeAll(ufrawTasks);
    }

    private static void findAndMatchFilesInDir(String dir) {
        System.out.println("Processing directory: " + dir);
        final boolean skip = fileLogger.isSuccess(dir);
        if (skip) {
            System.out.println("Skipping directory [previous success]: " + dir);
        }
        File folder = new File(dir);
        File[] files = folder.listFiles();

        if (!folder.exists() || files == null) {
            return;
        }

        ExecutorService parallelExecutor = Executors.newFixedThreadPool(4);
        List<File> filesToBeConverted = new LinkedList<>();
        Collection<CompletableFuture> async = new ArrayList<>();

        List<String> listOfFilePathsInDir = new ArrayList<>();
        for (File file : files) {
            listOfFilePathsInDir.add(file.getAbsolutePath());
        }

        for (File file : files) {
            if (file == null) continue;
            async.add(CompletableFuture.supplyAsync(() -> {
                if (file.isDirectory()) {
                    findAndMatchFilesInDir(file.getAbsolutePath());
                } else if (!skip && (file.getName().toLowerCase().endsWith(cr2) || file.getName().toLowerCase().endsWith(nef))) {
                    String existingJpgLcase = replaceExtension(file.getAbsolutePath(), "jpg");
                    String existingJpgUcase = replaceExtension(file.getAbsolutePath(), "JPG");
                    File existingJpg = null;
                    if (listOfFilePathsInDir.contains(existingJpgLcase)) {
                        existingJpg = new File(existingJpgLcase);
                    } else if (listOfFilePathsInDir.contains(existingJpgUcase)) {
                        existingJpg = new File(existingJpgUcase);
                    }
                    if (existingJpg != null) {
                        try {
                            Dimension jpgDimension = getImageDimension(existingJpg);
                            long width = (long) jpgDimension.getWidth();
                            long height = (long) jpgDimension.getHeight();
                            if (width < lowResThreshold || height < lowResThreshold) {
                                System.out.println(String.format("Existing JPG is low res - %s - %s [%dx%d]", file.getName(), existingJpg.getName(), width, height));
                                filesToBeConverted.add(file);
                            } else {
                                System.out.println(String.format("Existing JPG found - %s - %s [%dx%d]", file.getName(), existingJpg.getName(), width, height));
                                if (deleteRawOnHighResJpg) {
                                    System.out.println("Deleting raw [high res jpg found]: " + file.getAbsolutePath());
                                    if (!dryRun) file.delete();
                                }
                            }
                        } catch (IOException ioex) {
                            System.out.println("Error reading existing jpg: " + existingJpg.getAbsolutePath() + " - " + ioex.getMessage());
                            filesToBeConverted.add(file);
                        }
                    } else {
                        System.out.println(String.format("Existing JPG not found - %s", file.getName()));
                        filesToBeConverted.add(file);
                    }
                } else if (deleteXmpFiles && file.getName().toLowerCase().endsWith(xmp)) {
                    System.out.println("Deleting xmp: " + file.getAbsolutePath());
                    if (!dryRun) file.delete();
                }
                return null;
            }, parallelExecutor));
        }

        try {
            CompletableFuture.allOf(async.toArray(new CompletableFuture[]{})).get();
            mapOfDirToFiles.put(dir, filesToBeConverted);
            if (filesToBeConverted.isEmpty()) {
                fileLogger.recordSuccess(dir);
            }
        } catch (Exception ex) {
            System.err.println("Error while matching jpg in " + dir + " [message: " + ex.getMessage() + "]");
            fileLogger.log("ERROR: Unable to match JPG in " + dir + " [message: " + ex.getMessage() + "]");
            fileLogger.recordFailure(dir);
        }
    }

    private static void convertFiles(List<File> filesToBeConverted, String dir) {
        if (!filesToBeConverted.isEmpty()) {
            System.out.println("Number of raw files to be converted [" + dir + "]: " + filesToBeConverted.size());

            List<List<File>> subSets = ListUtils.partition(filesToBeConverted, 100);

            try {
                for (List<File> files : subSets) {
                    if (!dryRun) ufrawBatch(files);
                }
                if (deleteRawAfterConvert) {
                    for (File raw : filesToBeConverted) {
                        System.out.println("Deleting raw: " + raw.getAbsolutePath());
                        if (!dryRun) raw.delete();
                    }
                }
                fileLogger.log("SUCCESS: " + dir + " [files: " + filesToBeConverted.size() + "]");
                fileLogger.recordSuccess(dir);
            } catch (Throwable tr) {
                System.err.println("Error while processing " + dir + " [message: " + tr.getMessage() + "]");
                fileLogger.log("ERROR: " + dir + " [message: " + tr.getMessage() + "]");
                fileLogger.recordFailure(dir);
            }
        } else {
            fileLogger.log("SUCCESS: " + dir + " [no files converted]");
            fileLogger.recordSuccess(dir);
        }
    }

    private static void ufrawBatch(List<File> filesToBeConverted) throws IOException, InterruptedException {
        List<String> args = new LinkedList<>();
        args.add(ufrawBatch);
        for (File raw : filesToBeConverted) {
            args.add(raw.getAbsolutePath());
        }
        args.add(opExposure);
        args.add(opSaturation);
        args.add(opRestoreDetails);
        args.add(opClipFilm);
        args.add(opOutTypeJpg);
        args.add(opQuality);
        args.add(opNoOverwrite);
        args.add(opLensFun);
        args.add(opAutoCrop);
        args.add(opColorSmoothing);
        ProcessBuilder process = new ProcessBuilder();
        process.command(args.toArray(new String[]{}));
        process.redirectError(ProcessBuilder.Redirect.INHERIT);
        process.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        process.start().waitFor();
    }

    private static Dimension getImageDimension(File file) throws IOException {
        try(ImageInputStream in = ImageIO.createImageInputStream(file)){
            final Iterator<ImageReader> readers = ImageIO.getImageReaders(in);
            if (readers.hasNext()) {
                ImageReader reader = readers.next();
                try {
                    reader.setInput(in);
                    return new Dimension(reader.getWidth(0), reader.getHeight(0));
                } finally {
                    reader.dispose();
                }
            }
        }
        return new Dimension(0, 0);
    }

    private static String replaceExtension(String filename, String newExt) {
        int index = filename.lastIndexOf(".");
        String name = filename.substring(0, index);
        String ext = filename.substring(index);
        return name + "." + newExt;
    }
}
