package com.ers.pa1;

import java.io.*;
import java.net.Socket;

/**
 * This class is responsible to perform put operation requested by the server process. It will receive file content
 * over the network and will be written to a local file path for future retrieval.
 */
public class Fs533PutFileHelper extends Thread {

    private String serverIP;
    private int serverPort;
    private String fs533Key;

    public Fs533PutFileHelper(String _serverIP, int _serverPort, String _fs533Key) {
        super("PutThread");
        serverIP = _serverIP;
        serverPort = _serverPort;
        fs533Key = _fs533Key;
    }

    /**
     * This thread wait for put request of a specific file, read the content over the stream and write it to local file path.
     */
    @Override
    public void run() {
        String fileName = App.getInstance().fs533Client
                .randomFileGenerator("FILLER.data");
        File localFile = new File(fileName);
        App.LOGGER.info("Fs533PutFileHelper - run - put file: " + fileName);

        byte[] buffer = new byte[65536];
        int number;
        InputStream socketStream;
        try {
            Socket socket = new Socket(serverIP, serverPort);
            socketStream = socket.getInputStream();
            OutputStream fileStream = new FileOutputStream(localFile);
            int writeCounts = 0;
            while ((number = socketStream.read(buffer)) != -1) {
                fileStream.write(buffer, 0, number);
                writeCounts++;
            }
            App.LOGGER.info("Fs533PutFileHelper - run - put " + writeCounts + " writes for file: " + fileName);

            fileStream.close();
            socketStream.close();
            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        App.getInstance().fs533Client.updateFileMap(fs533Key, fileName);
    }

}
