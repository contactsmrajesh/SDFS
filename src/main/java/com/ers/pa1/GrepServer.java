package com.ers.pa1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * This program is responsible for accepting client connection for Grep Search, invoke Grep functionality
 * to execute the search and return the search result back the client.
 */

public class GrepServer {

    private Grep grep = new Grep();
    private volatile boolean running = false;
    private int port;
    private Thread monitorClientConn;


    /**
     * Constructor
     *
     * @param port to establish connection
     */
    public GrepServer(int port) {
        this.port = port;
    }

    /**
     * Start Grep server in all the Group Membership Nodes in designated port and wait for client connection.
     */
    public static void startGrepServer() {
        //System.out.println("Inside Grep Server");
        int port = App.GREP_SVC_PORT;
        GrepServer server = new GrepServer(port);
        server.start();
    }

    /**
     * Start client monitor thread
     */
    public void start() {
        if (running) return;
        running = true;
        monitorClientConn = new Thread(new GrepServer.GrepServerMonitorClient());
        monitorClientConn.start();
    }

    /**
     * Accept connection from client and start GrepServer to search the string.
     */
    class GrepServerMonitorClient implements Runnable {
        public void run() {
            try {
                ServerSocket ss = new ServerSocket(port, 5);
                while (running) {
                    // System.out.println("Started GrepServer at port " + port);
                    Socket s = ss.accept();
                    new Thread(new GrepServer.GrepServerSocket(s)).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Accepts the input search string with options and invoke Grep on the Application.log file.
     * Return the search results as output stream.
     */
    class GrepServerSocket implements Runnable {
        Socket socket;
        boolean run = true;

        public GrepServerSocket(Socket s) {
            this.socket = s;
        }

        @Override
        public void run() {
            try {
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String searchString = in.readLine();
                String ignoreCase = in.readLine();
                String reverseMatch = in.readLine();
                String printLineNum = in.readLine();

                out.println(grep.calPattern(searchString, ignoreCase, reverseMatch, printLineNum));


            } catch (IOException e) {
                System.out.println(e.getMessage());
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }
}
