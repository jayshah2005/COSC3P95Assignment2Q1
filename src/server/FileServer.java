package server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.*;

// import java.util.concurrent.*; // multithreading??? e.e

/*
    * FileServer - a simple TCP server that receives files from clients.
    *
    * Protocol (what the client sends):
    * 1. UTF string for file path ("" = done, stop receiving)
    * 2. long value for file size in bytes
    * 3. raw file bytes
    *
    * the server writes each received file under a chosen output folder.
 */

public class FileServer {

    private final int port;
    private final Path outputDir;

    /**
     * constructor - sets up the server configs
     *
     * @param port           the TCP port number to listen on
     * @param outputDir      the folder where incoming files will be stored
     */

    public FileServer(int port, Path outputDir) throws IOException {
        this.port = port;
        this.outputDir = outputDir;
        Files.createDirectories(outputDir);     // ensure the folder/directory exists (if not create it)
    }

    // ---------- main ----------
    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 5050;
        Path outDir = Paths.get(args.length > 1 ? args[1] : "server-out");

        // create server instance and start it
        FileServer server = new FileServer(port, outDir);
        server.start();
    }

    /*
        * starts the server: listens for client connections and handles them.
     */
    private void start() {
        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("[Server] Listening on port " + port);

            // infinite loop - waits for new clients
            while (true) {
                // when a client connects, accept() returns a Socket
                Socket client = server.accept();
                System.out.println("[Server] Client connected: " + client.getRemoteSocketAddress());

                // handle this client's data
                handleClient(client);
            }
        } catch (IOException e) {
            System.err.println("[Server] Error: " + e.getMessage());
        }
    }


    /*
        * reads incoming file data from a single client
        * only handles one client at a time (sequentially)
     */

    private void handleClient(Socket socket) {
        try (socket; DataInputStream in = new DataInputStream(
                new BufferedInputStream(socket.getInputStream()))) {
            while (true) {
                String relPath = in.readUTF();

                // empty string means client is done sending
                if (relPath.isEmpty()) break;

                long size = in.readLong();

                saveFile(in, relPath, size);
            }

            System.out.println("[Server] Client finished.");
        } catch (IOException e) {
            System.err.println("[Server] Client error: " + e.getMessage());
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
        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(target))) {
            byte[] buffer = new byte[8192];     // 8 KB buffer for reading chunks
            long remaining = size;              // bytes left to read

            while (remaining > 0) {
                // read up to buffer size or remaining bytes
                int bytesRead = in.read(buffer, 0, (int)Math.min(buffer.length, remaining));
                if (bytesRead == -1) { // watch: might cause an issue
                    throw new EOFException("Unexpected end of stream");
                }

                out.write(buffer, 0, bytesRead);
                remaining -= bytesRead;
            }
        }

        System.out.printf("[Server] Saved %s (%d bytes)%n", target, size);

    }
}

