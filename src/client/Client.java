package client;

import Configs.Config;

import java.io.*;
import java.net.Socket;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class that manages the GUI client
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
     * entry point for ruuning the client logic
     * - collect all files
     * - open a socket to the server
     * - send each file (path + size + bytes)
     * - send empty path "" to signal completion
     */
    public void run(){
        System.out.println("Client starting...");
        System.out.println("Connecting to " + host + ":" + port);

        try (Socket socket = new Socket(host, port);
        DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(socket.getOutputStream()))) {
            sendSingleFile(file, out);

            // send empty path to tell the server we are done
            out.writeUTF("");
            out.flush();

            System.out.println(file.getFileName() + " sent. Closing connection.");
        } catch (IOException e){
            System.out.println("Client error. Failed to send file: " + file.getFileName() + "\n" + e.getMessage());
        }
    }

    /**
     *  sends a single file to the server following the protocol:
     *  1) UTF-8 path (relative to inputDir, using '/'
     *  2) long size
     *  3) file bytes
     */
    private void sendSingleFile(Path file, DataOutputStream out) throws IOException {
        // make relative path so server can reconstruct directory structure
        String relative = file.getFileName().toString();

        long size = Files.size(file);

        System.out.println("Sending file: " + relative + " (" + size + " bytes)");


        try {
            // 1. Calculate checksum
            String checksum = calculateChecksum(file.toFile());
            out.writeUTF(checksum);

            // 2. send name
            out.writeUTF(relative);

            // 3. send size
            out.writeLong(size);

            // 4. send bytes in chunks
            InputStream in = new BufferedInputStream(Files.newInputStream(file));
            byte[] buffer = new byte[8192];
            long remaining = size;

            while (remaining > 0) {
                int toRead =  (int) Math.min(buffer.length, remaining);
                int read = in.read(buffer, 0, toRead);
                if (read == -1) {
                    break; // EOF reached unexpectedly, but won't hang
                }
                out.write(buffer, 0, read);
                remaining -= read;
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        out.flush();
        System.out.println("Finished sending: " + relative);
    }

    private static String calculateChecksum(File file) throws IOException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
        }
        return bytesToHex(digest.digest());
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}