package com.ers.pa1;

import org.apache.commons.cli.*;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * GrepClient class is implemented to launch the client side
 * allowing the user to query the regex pattern of interest and perform
 * grep in a distributed fashion.It establishes the connection with servers(multithreaded)
 * to get the matching lines per the user's query. It then generates client thread to
 * fetch the matching lines from server and counts the total number of lines returned.
 */

public class GrepClient extends Thread {

    public static PrintStream out;
    public static AtomicInteger totalLineCount;
    public static int totalNodes;


    private GrepClient() {
        out = System.out;
        totalLineCount = new AtomicInteger(0);
    }

    private GrepClient(PrintStream ps) {
        out = ps;
        totalLineCount = new AtomicInteger(0);
    }

    /**
     * Method to create GrepClient and set PrintStream Object
     *
     * @param ps
     * @return
     */
    public static GrepClient getNewInstance(PrintStream ps) {
        return new GrepClient(ps);
    }


    /**
     * Method to allow the user to pass the query with options as command line arguments
     *
     * @return An Option object
     */
    private static Options userQueryOptions() {
        Option regex = OptionBuilder.withArgName("regexString").hasArg().withDescription("*Required Grep query string").create("regex");
        Option v = new Option("v", "Option to reverse the match");
        Option i = new Option("i", "Case-insensitive option to ignore case");
        Option n = new Option("n", "Option to print the Line numbers");

        Options opt = new Options();
        opt.addOption(regex);
        opt.addOption(v);
        opt.addOption(i);
        opt.addOption(n);

        return opt;
    }

    /**
     * This method will be invoked from the App.java which the search string and options. It will parse the input
     * and invoke method to execute the Grep.
     *
     * @param args
     * @throws ParseException
     */
    public static void doGrep(String[] args) throws ParseException {
        GrepClient gc = new GrepClient();
        Options opt = userQueryOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine cl = null;
        try {
            cl = parser.parse(opt, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if (!cl.hasOption("regex")) {
            throw new ParseException(" regex query string should be passed ");
        } else {
            String queryString = cl.getOptionValue("regex");
            String ignoreCase = "N";
            String reverseMatch = "N";
            String printLineNum = "N";

            if (cl.hasOption("i"))
                ignoreCase = "Y";
            if (cl.hasOption("v"))
                reverseMatch = "Y";
            if (cl.hasOption("n"))
                printLineNum = "Y";
            gc.grepExec(queryString, ignoreCase, reverseMatch, printLineNum);
        }
    }

    /**
     * Method will iterate the Group Membership list and establish connection with each node
     * to perform the Grep search in the Application.log file
     *
     * @param query : regex pattern the user wishes to query
     * @param ic    : Option for the user's search to ignore the case
     * @param im    : Option for the user to invert search
     * @param pln   : Option for the user to view the Line Numbers
     */

    public void grepExec(String query, String ic, String im, String pln) {
        long start = System.currentTimeMillis();
        totalNodes = 0;

        ArrayList<GrepClientThread> threads = new ArrayList<GrepClientThread>(App.getInstance().groupMembershipService.nodeList.size());
        for (Machines node : App.getInstance().groupMembershipService.nodeList) {
            totalNodes++;
            GrepClientThread gT = new GrepClientThread(node.getIP(), query, ic, im, pln);
            threads.add(gT);
            gT.start();
        }
        try {
            for (GrepClientThread g : threads) {
                g.join();
            }
        } catch (InterruptedException e) {
            System.err.println("[ERROR] Interruption to the Client's thread waiting for the completion of the daemons.");
        } finally {
            System.out.println("Total Nodes Processed : " + totalNodes);
            System.out.println("Total line(s) : " + totalLineCount);
            long end = System.currentTimeMillis();
            NumberFormat formatter = new DecimalFormat("#0.00000");
            System.out.print("Total Execution time is " + formatter.format((end - start) / 1000d) + " seconds\n");
        }
    }

    /**
     * Generation of the Client threads to get the lines matched from the server
     * with the a check on the number of lines matched
     */
    private static class GrepClientThread extends Thread {
        String searchServer;
        String searchString;
        String ic;
        String im;
        String pln;

        public GrepClientThread(String server, String searchStr, String ic, String im, String pln) {
            this.searchServer = server;
            this.searchString = searchStr;
            this.ic = ic;
            this.im = im;
            this.pln = pln;
        }

        @Override
        public void run() {
            int lineCount = 0;
            Socket s = null;
            try {
                s = new Socket(searchServer, App.GREP_SVC_PORT);
                s.setSoTimeout(2000);
                //socket =  new Socket();
                //socket.connect(new InetSocketAddress(servers.nodeAddress, servers.nodePort), 2000);
            } catch (SocketTimeoutException se) {
                System.err.println("Socket time out");
                return;
            } catch (IOException e) {
                System.err.println("[ERROR] Connection to server can't be established " + searchServer);
                return;
            }
            Scanner readInput = null;
            try {
                readInput = new Scanner(new InputStreamReader(s.getInputStream()));
                readInput.useDelimiter("\n");
            } catch (IOException e) {
                System.err.println("[ERROR] Input stream connection failure from socket at client side");
                return;
            }
            PrintWriter writeOutput = null;
            try {
                writeOutput = new PrintWriter(new OutputStreamWriter(s.getOutputStream()));
            } catch (IOException e) {
                System.err.println("[ERROR] Creation of output stream failure from socket at client side");
                return;
            }

            writeOutput.println(this.searchString);
            writeOutput.println(this.ic);
            writeOutput.println(this.im);
            writeOutput.println(this.pln);
            writeOutput.flush();
            while (readInput.hasNext()) {
                GrepClient.out.println("[" + searchServer + "]:" + readInput.next());
                lineCount++;
            }
            totalLineCount.addAndGet(lineCount);
        }
    }
}