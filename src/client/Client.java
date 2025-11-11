package client;

import Configs.Config;

import java.io.*;
import java.net.Socket;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Class that manages the GUI client
 */
public class Client {

    private final int port;
    private final String host;
    private final Path inputDir;

    Client(int port, String host, Path inputDir) {
        this.port = port;
        this.host = host;
        this.inputDir = inputDir;
    }

    /**
     * entry point for ruuning the client logic
     * - collect all files
     * - open a socket to the server
     * - send each file (path + size + bytes)
     * - send empty path "" to signal completion
     */

    public void start(){
        System.out.println("Client starting...");
        System.out.println("Connecting to " + host + ":" + port);
        System.out.println("Input folder: " + inputDir.toAbsolutePath());

        try {
            List<Path> files = collectFiles(inputDir);
            if (files.isEmpty()) {
                System.out.println("No files found in " + inputDir + ". Nothing to send it seems");
                return;
            }

            try (Socket socket = new Socket(host, port);
            DataOutputStream out = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()))) {
                for (Path file : files) {
                    sendSingleFile(file, out);
                }

                // send empty path to tell the server we are done
                out.writeUTF("");
                out.flush();

                System.out.println("All files sent. Closing connection.");
            }
        } catch (IOException e) {
            System.out.println("Client error: " + e.getMessage());
//            e.printStackTrace();
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


    /**
     *  sends a single file to the server following the protocol:
     *  1) UTF-8 path (relative to inputDir, using '/'
     *  2) long size
     *  3) file bytes
     */

    private void sendSingleFile(Path file, DataOutputStream out) throws IOException {
        // make relative path so server can reconstruct directory structure
        String relative = inputDir.relativize(file).toString().replace(File.separatorChar, '/');

        long size = Files.size(file);

        System.out.println("Sending file: " + relative + " (" + size + " bytes)");

        // 1) send path
        out.writeUTF(relative);

        // 2) send size
        out.writeLong(size);

        // 3) send bytes in chunks
        try (InputStream in = new BufferedInputStream(Files.newInputStream(file))) {
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
        }

        out.flush();
        System.out.println("Finished sending: " + relative);
    }




//    /**
//     * TODO: Figure out a way to send all files over to the server
//     * @return a boolean if files were sent successfully
//     */
//    Boolean sendFiles(){
//
//        if(!client.isConnected()){
//            System.out.println("Client not connected");
//            return false;
//        }
//
//        DataOutputStream out;
//        InputStream in;
//
//        try {
//            out = new DataOutputStream();
//            in = client.getInputStream();
//
//
//        } catch (IOException e) {
//            System.out.println("Client failed to connect: " + e.getMessage());
//        }
//
//        return true;
//    }

    public static void main(String[] args) {

        Config config = Config.getInstance();
        int port = config.port;
        String host = config.host;

        // let the user pass a custom input folder as the first argument
        Path inDir = Paths.get(args.length > 0 ? args[0] : "client-data");

        Client client = new Client(port, host, inDir);
        client.start();
    }
}