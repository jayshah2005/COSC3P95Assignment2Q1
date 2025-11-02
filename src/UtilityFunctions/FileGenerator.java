package UtilityFunctions;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

public class FileGenerator {

    private String folderName;
    private int numFiles;
    private Random random;

    public FileGenerator(String folderName, int numFiles) {
        this.folderName = folderName;
        this.numFiles = numFiles;
        this.random = new Random();
    }

    public FileGenerator() {
        this("clientFiles", 20);
    }

    /**
     * Generates a random file size
     * @return a random file size
     */
    private long generateFileSize() {

        // Randomly pick small, medium, or large
        int category = random.nextInt(3); // 0: small, 1: medium, 2: large

        long sizeKB;
        switch (category) {
            case 0: // small: 5 KB to 100 KB
                sizeKB = 5 + random.nextInt(96);
                break;
            case 1: // medium: 100 KB to 10 MB
                sizeKB = 100 + random.nextInt(10 * 1024 - 100);
                break;
            default: // large: 10 MB to 100 MB
                sizeKB = 10 * 1024 + random.nextInt(90 * 1024);
        }

        return sizeKB;
    }

    /**
     * Create a folder to write files to
     */
    public void createFolder() {
        File folder = new File(folderName);
        if (folder.mkdirs()) {
            System.out.println("Created folder: " + folderName);
        }
    }

    /**
     * Generates random files
     */
    public void generateFiles() {
        createFolder();

        for (int i = 1; i <= numFiles; i++) {
            long sizeKB = generateFileSize();
            File file = new File(folderName + File.separator + "file_" + i + ".bin");
            System.out.printf("Generating %s (%.2f MB)...%n", file.getName(), sizeKB / 1024.0);

            try {
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                raf.setLength(sizeKB);
            } catch (IOException e) {
                System.err.println("Failed to generate file: " + file.getName());
                e.getMessage();
            }
        }

        System.out.println("All files generated successfully.");
    }

    public static void main(String[] args) {
        FileGenerator generator = new FileGenerator();
        generator.generateFiles();
    }
}