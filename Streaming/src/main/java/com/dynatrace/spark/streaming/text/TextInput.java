package com.dynatrace.spark.streaming.text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class TextInput {

    public static void main(String[] args) {

        try (BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
             ServerSocket serverSocket = new ServerSocket(1234)) {

            System.out.println("Waiting for connection");
            Socket socket = serverSocket.accept();
            System.out.println("Connection accepted.");
            OutputStream outputStream = socket.getOutputStream();

            String s;
            do {
                s = r.readLine();
                System.out.println(s);
                if (s != null) {
                    outputStream.write((s + "\n").getBytes());
                }
            } while (s != null && !s.isEmpty());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
