package server;

import java.io.*;
import java.net.*;
import java.rmi.ServerException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;

class ProcessReq extends Thread {
    Socket socket;
    DataInputStream input;
    DataOutputStream output;
    Gson gson;
    JsonServer db;
    AtomicBoolean exitFlag;
    ServerSocket parentServer;

    public ProcessReq(Socket socket, JsonServer db, AtomicBoolean flag, ServerSocket parent) {
        try {
            this.parentServer = parent;
            this.gson = new Gson();
            this.socket = socket;
            this.input = new DataInputStream(socket.getInputStream());
            this.output = new DataOutputStream(socket.getOutputStream());
            this.db = db;
            this.exitFlag = flag;
        } catch (IOException e) {
            System.err.println("Couldn't establish connection. Thread " + this.getName());
        }
    }

    public void run() {
        try {
            String cmd = input.readUTF();
            JsonObject cmdObj = gson.fromJson(cmd, JsonObject.class);
            //Map<String, String> response = new HashMap<>();
            JsonObject response = new JsonObject();

            if (cmdObj.get("type").getAsString().equalsIgnoreCase("exit")) {
                System.out.println("Received: " + cmd);
                exitFlag.set(true);
                response.addProperty("response", "OK");
                parentServer.close();
            } else {
                System.out.println("Received: " + cmd);
                response = db.execute(cmdObj);
            }
            String responseStr = gson.toJson(response);
            output.writeUTF(responseStr);
            System.out.println("Sent: " + responseStr);
        } catch (IOException e) {
            System.err.println("Could not process request: " + this.getName());
        }
    }
}

public class Main {

    //static AtomicBoolean exitFlag;

    public static void main(String[] args) {
        System.out.println("Server started!");
        String filepath = System.getProperty("user.dir") + "/src/server/data";
        JsonServer database = new JsonServer(filepath);

        String address = "127.0.0.1";
        int port = 23456;
        try {
            ServerSocket serverSocket = new ServerSocket(port, 50, InetAddress.getByName(address));
            ExecutorService exService = Executors.newFixedThreadPool(4);
            AtomicBoolean exitFlag = new AtomicBoolean(false);

            while (!exitFlag.get()) {
                try {
                    Socket socket = serverSocket.accept();
                    exService.submit(new ProcessReq(socket, database, exitFlag, serverSocket));
                } catch (SocketException e) {

                }
            }
            serverSocket.close();
            exService.shutdown();
            boolean terminated = false;
            try {
                terminated = exService.awaitTermination(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                System.err.println("ExecutionService shutdown was interrupted.");
            }
            if (terminated) {
                System.out.println("ExecutionService shutdown successful.");
            } else {
                System.out.println("Timeout elapsed before ExecutionService could terminate.");
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (!database.shutdown()) {
            System.err.println("Database didn't shut down correctly.");
        }
    }
}