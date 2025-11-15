package client;

import Configs.Config;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import telemetry.TelemetryConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Class to run all the clients together
 */
public class ClientRunner {

    private final int port;
    private final String host;
    private final Path inputDir;
    List<Path> files;

    /**
     *
     * @param port the port all the clients will conenct to
     * @param host the hostname that client will connect to
     * @param inputDir the input directory of all the files
     */
    ClientRunner(int port, String host, Path inputDir) {
        this.port = port;
        this.host = host;
        this.inputDir = inputDir;
        this.getFiles();
    }

    /**
     * Run all the clients
     */
    public void run() throws IOException {
        for(Path file : this.files){
            Client c = new Client(port, host, file);
            new Thread(c).start();
        }
    }

    /**
     * Get files from the input directory
     */
    private void getFiles() {

        System.out.println("Input folder: " + inputDir.toAbsolutePath());
        try {
            this.files = collectFiles(inputDir);
            if (files.isEmpty()) {
                System.out.println("No files found in " + inputDir + ". Nothing to send it seems");
            }
        } catch (IOException e) {
            System.out.println("Client error: " + e.getMessage());
        }
    }

    /**
     * recursively collects all regular files under the given directory
     * @param root the root directory to start collecting from
     */
    private List<Path> collectFiles(Path root) throws IOException {
        List<Path> files = new ArrayList<>();
        if(!Files.exists(root)) {
            System.out.println("Input directory does not exist: " + root);
            return files;
        }

        try (var stream = Files.walk(root)) {
            stream.filter(Files::isRegularFile)
                    .forEach(files::add);
        }

        System.out.println("Found " + files.size() + " file(s) to send.");
        return files;
    }

    public static void main(String[] args) {
        Config config = Config.getInstance();
        int port = config.port;
        String host = config.host;
        Path inDir = Paths.get(args.length > 0 ? args[0] : "files2transfer");


        /**
         * OpenTelemtry init for CLIENT
         * 1.0 = AlwaysOn sampling
         */
        TelemetryConfig.init("file-server", 1.0); // AlwaysOn for now

        // Tiny test span just to prove it works
        Tracer tracer = TelemetryConfig.tracer();
        Span startup = tracer.spanBuilder("server.startup_test").startSpan();
        startup.end();


        ClientRunner cl = new ClientRunner(port, host, inDir);

        try {
            cl.run();
            System.out.println("Files sent successfully.");
        } catch (IOException e) {
            System.out.println("Exiting client runner.");
        }
    }
}
