package client;


import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;


public class Main {

    @Parameter(names = {"-t"}, description = "Type of request")
    String cmdType;

    @Parameter(names = {"-k"}, description = "Key to be used")
    String index;

    @Parameter(names = {"-v"}, description = "Value to be saved")
    String value;

    @Parameter(names = {"-in"}, description = "File containing request")
    String requestFileName;

    public static void main(String[] argv) {
        Main main = new Main();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(argv);
        main.run();
    }

    public void run() {

        Gson gson = new Gson();
        String address = "127.0.0.1";
        int port = 23456;
        try (Socket socket = new Socket(InetAddress.getByName(address), port);
             DataInputStream input = new DataInputStream(socket.getInputStream());
             DataOutputStream output = new DataOutputStream(socket.getOutputStream())
        ) {
            System.out.println("Client started!");
            Map<String, String> cmdMap = new HashMap<>();
            if (requestFileName != null) {
                String dirpath = System.getProperty("user.dir") + "/src/client/data/";
                try (JsonReader jsonReader = new JsonReader(new FileReader(dirpath + requestFileName))) {
                    cmdMap = gson.fromJson(jsonReader, java.util.HashMap.class);
                } catch (IOException e) {
                    System.err.println("Couldn't open request file: " + requestFileName);
                    System.err.println("Searched at: " + dirpath + requestFileName);
                    System.exit(1);
                }
            } else {
                cmdMap.put("type", cmdType);
                if (cmdType.equalsIgnoreCase("get")) {
                    cmdMap.put("key", String.valueOf(index));
                } else if (cmdType.equalsIgnoreCase("set")) {
                    cmdMap.put("key", String.valueOf(index));
                    cmdMap.put("value", value);
                } else if (cmdType.equalsIgnoreCase("delete")) {
                    cmdMap.put("key", String.valueOf(index));
                }
            }
            String cmd = gson.toJson(cmdMap);
            output.writeUTF(cmd);
            System.out.println("Sent: " + cmd);
            String response = input.readUTF();
            System.out.println("Received: " + response);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}