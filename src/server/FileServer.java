package server;

import Configs.Config;

import telemetry.TelemetryConfig;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.Span;


//import io.opentelemetry.api.OpenTelemetry;


import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.*;

// import java.util.concurrent.*; // multithreading??? e.e

/**
    * FileServer - a multithreaded TCP server that can receives files from multiple clients.
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

    /**
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

                ServerThread serverThread = new ServerThread(client, outputDir);
                System.out.println("[Server] New Server Thread Started");
                new Thread(serverThread).start();
            }
        } catch (IOException e) {
            System.err.println("[Server] Error: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {

        Config config = Config.getInstance();

        int port = config.port;
        Path outDir = Paths.get(args.length > 1 ? args[1] : "server-out");


        /**
         * OpenTelemtry init for CLIENT
         * 1.0 = AlwaysOn sampling
         */
        TelemetryConfig.init("file-server", 1.0); // AlwaysOn for now

        // Tiny test span just to prove it works
        Tracer tracer = TelemetryConfig.tracer();
        Span startup = tracer.spanBuilder("server.startup_test").startSpan();
        startup.end();



        // create server instance and start it
        FileServer server = new FileServer(port, outDir);
        server.start();
    }
}

