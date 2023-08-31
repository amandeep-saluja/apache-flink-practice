package com.example.flink.socketExample;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;

public class SocketRead {

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<String> ds = env.socketTextStream("localhost", 9000, "\n");
            ds.print();
            env.execute();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runWithSocket() {
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
            e.printStackTrace();
        }
    }
}
