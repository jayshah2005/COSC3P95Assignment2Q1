package server;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A thread to handle a client
 */
public class ServerThread implements Runnable {

    final Socket socket;
    private final Path outputDir;
    String originalChecksum;


    ServerThread(Socket socket, Path outputDir) {
        this.socket = socket;
        this.outputDir = outputDir;
    }

    /**
     * reads incoming file data from a single client
     * only handles one client at a time (sequentially)
     */
    @Override
    public void run() {
        try (this.socket; DataInputStream in = new DataInputStream(
                new BufferedInputStream(this.socket.getInputStream()))) {
            while (in.available() != -1) {

                // Get the original checksum
                originalChecksum = in.readUTF();
                String relPath = in.readUTF();

                // empty string means client is done sending
                if (relPath.isEmpty()) break;

                long size = in.readLong();

                saveFile(in, relPath, size);
            }

            System.out.println("[Server] Client finished.");
        } catch (EOFException eof) {
            System.out.println("[Server] Client disconnected normally.");
        } catch (SocketException se) {
            System.out.println("[Server] Connection reset by client.");
        } catch (Exception e) {
            System.out.println("[Server] Unexpected error: " + e);
        }
}

    /**
     * saves one file from the input stream to the output directory
     *
     * @param in        the input stream to from the client
     * @param relPath   the relative path (e.g., "data/test.txt")
     * @param size      the file size in the bytes
     */
    private void saveFile(DataInputStream in, String relPath, long size) throws IOException {

        // resolve target path relative to outputDir
        Path target = outputDir.resolve(relPath).normalize();

        // prevent writing files outside outputDir (for security)
        if (!target.startsWith(outputDir)) {
            throw new IOException("Blocked unsafe path: " + relPath);
        }

        // make sure parent folders exist (e.g., "server-out/data/")
        if (target.getParent() != null) {
            Files.createDirectories(target.getParent());
        }

        // create file output stream and copy bytes from the network
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            OutputStream out = new BufferedOutputStream(Files.newOutputStream(target));
            byte[] buffer = new byte[8192];     // 8 KB buffer for reading chunks
            long remaining = size;              // bytes left to read

            while (remaining > 0) {
                // read up to buffer size or remaining bytes
                int bytesRead = in.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (bytesRead == -1) { // watch: might cause an issue
                    throw new EOFException("Unexpected end of stream");
                }

                out.write(buffer, 0, bytesRead);
                digest.update(buffer, 0, bytesRead);
                remaining -= bytesRead;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Compare the checksums
        String computedChecksum = bytesToHex(digest.digest());
        if (computedChecksum.equals(originalChecksum)) {
            System.out.println("✅ File received successfully. Checksum matches.");
        } else {
            System.out.println("❌ Checksum mismatch! File may be corrupted.");
            System.out.println("Expected: " + originalChecksum);
            System.out.println("Received: " + computedChecksum);
        }
        System.out.printf("[Server] Saved %s (%d bytes)%n", target, size);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
