package com.ers.pa1;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * This class implements Get Helper functionality. If there is a request for a file from server, it will get
 * the local file location and stream the file content over a socket connection.
 */
public class Fs533GetFileHelper extends Thread {

    private String serverIP;
    private int serverPort;
    private String fs533Key;

    public Fs533GetFileHelper(String _serverIP, int _serverPort, String _fs533Key) {
        super("GetThread");
        serverIP = _serverIP;
        serverPort = _serverPort;
        fs533Key = _fs533Key;
    }

    /**
     * Receives file request, perform a lookup and stream the file content over the network if found.
     */
    @Override
    public void run() {
        Socket socket = null;
        try {
            socket = new Socket(serverIP, serverPort);
        } catch (UnknownHostException e3) {
            e3.printStackTrace();
        } catch (IOException e3) {
            e3.printStackTrace();
        }
        byte[] buffer = new byte[65536];
        int number;
        String localFile = App.getInstance().fs533Client.getFileLocation(fs533Key);

        OutputStream socketOutputStream = null;
        try {
            socketOutputStream = socket.getOutputStream();
        } catch (IOException e2) {
            // TODO Auto-generated catch block
            e2.printStackTrace();
        }

        if (localFile == null) {
            localFile = App.FS533_DIR + File.separator + App.hostaddress + "_" + UUID.randomUUID().toString() + ".data";
        }

        File fs533File = new File(localFile);
        if (!fs533File.exists()) {
            try {
                fs533File.createNewFile();
            } catch (IOException e) {
                System.out
                        .println("Unable to create File, check permission!");
                e.printStackTrace();
                return;
            }
        }

        InputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(fs533File);
        } catch (IOException e2) {
            e2.printStackTrace();
        }


        try {
            while ((number = fileInputStream.read(buffer)) != -1) {
                try {
                    socketOutputStream.write(buffer, 0, number);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        try {
            socketOutputStream.close();
            fileInputStream.close();
            socket.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
