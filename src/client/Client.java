package client;


import java.io.*;
import java.net.Socket;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.GZIPOutputStream;

import telemetry.TelemetryConfig;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import io.opentelemetry.api.trace.StatusCode;

/**
 * Client responsible for sending a single file to the server
 * one client instance is run per file (from ClientRunner)
 */
public class Client implements Runnable {

    private final int port;
    private final String host;
    private final Path file;

    Client(int port, String host, Path file) {
        this.port = port;
        this.host = host;
        this.file = file;
    }

    /**
     * entry point for running the client logic
     */
    public void run() {
        System.out.println("Client starting...");
        System.out.println("Connecting to " + host + ":" + port);

        Tracer tracer = TelemetryConfig.tracer();

        // span for the whole client session (one file per client)
        Span sessionSpan = tracer.spanBuilder("client.sent_file_session")
                .setAttribute("server.host", host)
                .setAttribute("server.port", port)
                .startSpan();

        try (Scope scope = sessionSpan.makeCurrent();
                Socket socket = new Socket(host, port);
                DataOutputStream out = new DataOutputStream(
                        new BufferedOutputStream(socket.getOutputStream()))
            ) {
                System.out.println("Connected. Sending file: " + file.getFileName());

                // Send the file
                sendSingleFile(file, out);

                // Signal that no more files will be sent
                out.writeUTF("");
                out.flush();

                System.out.println("File sent successfully. Closing connection.");

            } catch (IOException e) {
                sessionSpan.recordException(e);
                sessionSpan.setStatus(StatusCode.ERROR, e.getMessage());
                System.out.println("Client error: Failed to send file " + file.getFileName() + "\n" + e.getMessage());
            } finally {
                sessionSpan.end();
            }

    }


    /**
     *  sends a single file to the server
     */
    private void sendSingleFile(Path file, DataOutputStream out) throws IOException {
        Tracer tracer = TelemetryConfig.tracer();
        String filename = file.getFileName().toString();
        long originalSize = Files.size(file);

        Span fileSpan = tracer.spanBuilder("client.send_file")
                .setAttribute("file.name", filename)
                .setAttribute("file.original_size", originalSize)
                .startSpan();

        try (Scope scope = fileSpan.makeCurrent()) {
            System.out.println("Compressing & sending file: " + filename);

            // 1. Read + compress file into byte[]
            fileSpan.addEvent("compression_start");
            byte[] compressedBytes = compressFile(file);
            fileSpan.addEvent("compression_end");

            long compressedSize = compressedBytes.length;
            fileSpan.setAttribute("file.compressed_size", compressedSize);
            fileSpan.setAttribute("compression_ratio",
                    (double) compressedSize / (double) originalSize);

            // 2. Calculate checksum after compression
            String checksum;
            try {
                checksum = calculateChecksum(compressedBytes);
            } catch (NoSuchAlgorithmException e) {
                fileSpan.recordException(e);
                fileSpan.setStatus(StatusCode.ERROR, e.getMessage());
                throw new RuntimeException(e);
            }

            // --- SEND METADATA ---
            fileSpan.addEvent("send_metadata_start");
            out.writeUTF(checksum);          // compressed checksum
            out.writeUTF(filename);          // original filename
            out.writeLong(compressedSize);   // compressed size
            fileSpan.addEvent("send_metadata_end");

            // --- SEND COMPRESSED BYTES IN CHUNKS ---
            long remaining = compressedSize;
            byte[] buffer = new byte[8192];
            int offset = 0;

            while (remaining > 0) {
                int chunk = (int) Math.min(buffer.length, remaining);
                System.arraycopy(compressedBytes, offset, buffer, 0, chunk);

                out.write(buffer, 0, chunk);

                offset += chunk;
                remaining -= chunk;
            }

            out.flush();
            fileSpan.addEvent("send_data_end");

            System.out.println("Finished sending compressed: " + filename +
                    " (" + compressedSize + " bytes)");
        } catch (IOException e) {
            fileSpan.recordException(e);
            fileSpan.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            fileSpan.end();
        }

//        System.out.println("Compressing & sending file: " + filename);
//
//        // 1. Read + compress file into byte[]
//        byte[] compressedBytes = compressFile(file);
//        long compressedSize = compressedBytes.length;
//
//        // 2. Calculate checksum after compression
//        String checksum;
//        try {
//            checksum = calculateChecksum(compressedBytes);
//        } catch (NoSuchAlgorithmException e) {
//            throw new RuntimeException(e);
//        }
//
//        // --- SEND METADATA ---
//        out.writeUTF(checksum);          // compressed checksum
//        out.writeUTF(filename);          // original filename
//        out.writeLong(compressedSize);   // compressed size
//
//        // --- SEND COMPRESSED BYTES IN CHUNKS ---
//        long remaining = compressedSize;
//        byte[] buffer = new byte[8192];
//        int offset = 0;
//
//        while (remaining > 0) {
//            int chunk = (int) Math.min(buffer.length, remaining);
//            System.arraycopy(compressedBytes, offset, buffer, 0, chunk);
//
//            out.write(buffer, 0, chunk);
//
//            offset += chunk;
//            remaining -= chunk;
//        }
//
//        out.flush();
//        System.out.println("Finished sending compressed: " + filename +
//                " (" + compressedSize + " bytes)");
    }

    /**
     * Compress a given file
     * @param path the path where the file is located
     */
    private byte[] compressFile(Path path) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (GZIPOutputStream gzip = new GZIPOutputStream(baos);
             InputStream in = new BufferedInputStream(Files.newInputStream(path))) {

            byte[] buffer = new byte[8192];
            int read;

            while ((read = in.read(buffer)) != -1) {
                gzip.write(buffer, 0, read);
            }
        }

        return baos.toByteArray();
    }

    /**
     * Calculate checksum for a given array of bytes
     */
    private String calculateChecksum(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(data);

        StringBuilder sb = new StringBuilder();
        for (byte b : hash) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}