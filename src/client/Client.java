package client;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.api.GlobalOpenTelemetry; // Accesses the configured Tracer

import java.io.*;
import java.net.Socket;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.GZIPOutputStream;

/**
 * Class that manages the GUI client with OpenTelemetry tracing.
 */
public class Client implements Runnable {

    // Get the global Tracer instance
    private static final Tracer tracer = GlobalOpenTelemetry.getTracer("file-client-instrumentation", "1.0.0");

    private final int port;
    private final String host;
    private final Path file;

    Client(int port, String host, Path file) {
        this.port = port;
        this.host = host;
        this.file = file;
    }

    /**
     * Entry point for running the client logic. Wrapped in a root span.
     */
    public void run() {
        // 1. Start a root span for the entire client execution
        Span rootSpan = tracer.spanBuilder("ClientRunner").startSpan();

        // 2. Make the span active in the current execution context
        try (Scope scope = rootSpan.makeCurrent()) {

            System.out.println("Client starting...");
            rootSpan.setAttribute("peer.address", host + ":" + port);

            // Start a child span for the connection attempt
            Span connectSpan = tracer.spanBuilder("Connect to Server").startSpan();
            try (Scope connectScope = connectSpan.makeCurrent()) {
                System.out.println("Connecting to " + host + ":" + port);

                try (
                        Socket socket = new Socket(host, port);
                        DataOutputStream out = new DataOutputStream(
                                new BufferedOutputStream(socket.getOutputStream()))
                ) {
                    connectSpan.setAttribute("net.peer.name", host);
                    connectSpan.setAttribute("net.peer.port", port);
                    connectSpan.setAttribute("status", "connected"); // Custom attribute
                    connectSpan.end(); // Finish connection span

                    System.out.println("Connected. Sending file: " + file.getFileName());

                    // Send the file
                    sendSingleFile(file, out);

                    // Signal that no more files will be sent
                    out.writeUTF("");
                    out.flush();

                    System.out.println("File sent successfully. Closing connection.");

                } catch (IOException e) {
                    // Record exception on the main root span
                    rootSpan.recordException(e);
                    rootSpan.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, "Failed to send file");

                    System.out.println("Client error: Failed to send file "
                            + file.getFileName() + "\n" + e.getMessage());
                }
            } finally {
                // Ensure the connection span ends even if an error occurs
                if (connectSpan.isRecording()) {
                    connectSpan.end();
                }
            }
        } finally {
            // 3. Always end the root span
            rootSpan.end();
        }
    }

    /**
     * Sends a single file to the server. Now wrapped in its own span.
     */
    private void sendSingleFile(Path file, DataOutputStream out) throws IOException {
        String filename = file.getFileName().toString();

        Span sendFileSpan = tracer.spanBuilder("sendSingleFile").startSpan();
        sendFileSpan.setAttribute("file.name", filename);
        sendFileSpan.setAttribute("file.original.size", Files.size(file));

        try (Scope scope = sendFileSpan.makeCurrent()) {

            System.out.println("Compressing & sending file: " + filename);

            // 1. Read + compress file into byte[] (wrapped in a child span)
            byte[] compressedBytes = compressFile(file);
            long compressedSize = compressedBytes.length;
            sendFileSpan.setAttribute("file.compressed.size", compressedSize);

            // 2. Calculate checksum after compression (wrapped in a child span)
            String checksum = calculateChecksumTraced(compressedBytes);

            // --- SEND METADATA ---
            out.writeUTF(checksum);          // compressed checksum
            out.writeUTF(filename);          // original filename
            out.writeLong(compressedSize);   // compressed size

            // --- SEND COMPRESSED BYTES IN CHUNKS ---
            Span transferSpan = tracer.spanBuilder("Transfer Data Chunks").startSpan();
            try (Scope transferScope = transferSpan.makeCurrent()) {
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
            } finally {
                transferSpan.end();
            }

            out.flush();
            System.out.println("Finished sending compressed: " + filename +
                    " (" + compressedSize + " bytes)");

        } catch (NoSuchAlgorithmException e) {
            sendFileSpan.recordException(e);
            sendFileSpan.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR);
            throw new IOException("Checksum error", e);
        } finally {
            sendFileSpan.end(); // Finish send file span
        }
    }

    /**
     * Compress a given file. Now wrapped in its own span.
     */
    private byte[] compressFile(Path path) throws IOException {
        Span compressSpan = tracer.spanBuilder("compressFile (GZIP)").startSpan();
        try (Scope scope = compressSpan.makeCurrent()) {
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
        } finally {
            compressSpan.end();
        }
    }

    /**
     * Calculate checksum for a given array of bytes. Now wrapped in its own span.
     */
    private String calculateChecksumTraced(byte[] data) throws NoSuchAlgorithmException {
        Span checksumSpan = tracer.spanBuilder("calculateChecksum (SHA-256)").startSpan();
        try (Scope scope = checksumSpan.makeCurrent()) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);

            StringBuilder sb = new StringBuilder();
            for (byte b : hash) sb.append(String.format("%02x", b));
            return sb.toString();
        } finally {
            checksumSpan.end();
        }
    }
}