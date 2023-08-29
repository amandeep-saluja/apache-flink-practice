package com.example.flink.socket;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;

public class SocketRead {

    public static void main(String[] args) {
        try {
            int port = 9000;  // Specify the port you want to connect to
            String host = "localhost";
            Socket socket = new Socket(host, port);

            InputStream inputStream = socket.getInputStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            String inputData;
            while ((inputData = bufferedReader.readLine())!=null) {
                System.out.println("Received: "+inputData);
            }
            bufferedReader.close();
            inputStreamReader.close();
            inputStream.close();
            socket.close();
        }catch (Exception e) {
            System.out.println(e);
        }
    }
}
