package com.ers.pa1;

import org.apache.commons.cli.ParseException;

import java.io.*;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.logging.*;

/**
 * This is the entry point for the FS533 distributed file system Application which also leverages the Group Membership Service
 * and Grep Search Functionality.This program will be initiated in all the nodes which needs to join the
 * group membership service and perform the required file operations (put, get, remove, list & locate)
 * along with grep search among the group member nodes. The user will be prompted to opt for the operation of choice.
 */

public class App {

    public static final int UDP_LISTEN_PORT = 5000;
    public static final int GREP_SVC_PORT = 4500;
    public static final int timeBoundedFailureInMilli = 5000;
    public static final String GATEWAYNODE_IP = "172.31.51.49";
    public static final int UDP_HB_PORT = 4464;
    public static final int TCP_FS533_PORT = 6000;
    public static String hostaddress = "";
    public static Logger LOGGER;
    public static String FS533_DIR = "";
    public static String LOG_DIR = "";
    public static boolean verbose = true;
    private static App instance = null;
    private static Handler logFileHandler;
    public GroupMembershipService groupMembershipService;
    public GroupMemberGateway groupGateway;
    public Fs533Client fs533Client;
    public Fs533Server fs533Server;
    protected DatagramSocket socket = null;
    private String rootLocation = "";
    private HeartBeatFDServer heartBeatFDServer;
    private HeartBeatFDClient heartBeatFDClient;
    private Socket clientSocket = null;

    private App() {
        this.groupMembershipService = new GroupMembershipService(App.hostaddress);
    }

    public static App getInstance() {
        if (instance == null) {
            instance = new App();
        }
        return instance;
    }

    /**
     * This is the main program which will create the App object, configure log, initialize Gateway Service
     * and prompt for user inputs.
     *
     * @param args
     */
    public static void main(String[] args) {
        double messageFailureRate = 0.0;
        App appObj = null;

        try {
            App.hostaddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
        }
        appObj = App.getInstance();
        appObj.configure(messageFailureRate);
        appObj.execute();
    }

    /**
     * Configures/starts the failure detection threads and sets the intentional
     * message failure rate
     */

    public void configure(double messageFailureRate) {
        String hostname = "localhost";
        try {
            hostname = java.net.InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e1) {
            LOGGER.warning("GroupMemberGateway - Membership Join request from the Client failed");
        }
        //App.LOG_DIR = "C:\\Raj\\Course\\Term2\\ErrorResilientSystems\\logs";
        this.rootLocation = "/home/ec2-user/";
        App.LOG_DIR = this.rootLocation + File.separator + "logs";
        App.FS533_DIR = this.rootLocation + File.separator + "fs533";
        Process p;
        boolean completed = false;
        while (!completed) {
            try {
                p = Runtime.getRuntime().exec("mkdir -p " + App.LOG_DIR);
                p.waitFor();
                p = Runtime.getRuntime().exec("mkdir -p " + App.FS533_DIR);
                p.waitFor();
                completed = true;
            } catch (IOException e1) {
                e1.printStackTrace();
                break;
            } catch (InterruptedException e) {
                // Exception occured
            }
        }
        String logFileLocation = App.LOG_DIR + "/Application.log";

        try {
            logFileHandler = new FileHandler(logFileLocation);
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        LOGGER = Logger.getLogger("App");
        LOGGER.setUseParentHandlers(false);
        logFileHandler.setFormatter(new SimpleFormatter());
        logFileHandler.setLevel(Level.INFO);
        LOGGER.addHandler(logFileHandler);

        if (messageFailureRate <= 0.0 || messageFailureRate > 1.0) {
            HeartBeatFDClient.msgFailureRate = 0.0;
        } else {
            HeartBeatFDClient.msgFailureRate = messageFailureRate;
        }
        this.heartBeatFDServer = new HeartBeatFDServer();
        this.heartBeatFDClient = new HeartBeatFDClient();
        this.fs533Client = new Fs533Client();
        this.fs533Server = new Fs533Server();
        this.groupGateway = new GroupMemberGateway();
    }

    /**
     * Method to initialize the membership if it is gateway node and start group membership services
     * Displays various options to users to join/leave/print/search group membership list
     */
    public void execute() {
        if (hostaddress.equals(GATEWAYNODE_IP)) {
            synchronized (groupMembershipService) {
                Machines node = new Machines(hostaddress, UDP_LISTEN_PORT);
                groupMembershipService.addNodeToList(node);
            }
        }
        startServices();

        InputStreamReader inputStreamReader = new InputStreamReader(System.in);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String input = "";

        while (true) {
            getUserInput();

            try {
                input = bufferedReader.readLine();
                if ("j".equals(input.trim())) {
                    if (!App.getInstance().groupMembershipService.nodeList.contains(hostaddress)) {
                        groupGateway.joinGroupMemberService();
                    } else {
                        System.out.println("Node " + hostaddress
                                + " is already part of the group membership list.");
                    }
                } else if ("p".equals(input.trim())) {
                    System.out.println(this.groupMembershipService.toString());
                } else if ("l".equals(input.trim())) {
                    System.out.println("Leaving Group Membership Service");
                    leaveGroup();
                } else if ("e".equals(input.trim())) {
                    System.out.println("Exit the application");
                    stopGroupServices();
                    System.exit(0);
                } else if ("s".equals(input.trim())) {
                    ArrayList<String> al = new ArrayList<String>();
                    System.out.println("Enter Search String in format <string> <-ivn>");
                    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                    String inputStr = reader.readLine();
                    al.add("-regex");
                    for (int i = 0; i < inputStr.split(" ").length; i++) {
                        al.add(inputStr.split(" ")[i]);
                    }
                    String[] ipArr = getStringArray(al);
                    GrepClient.doGrep(ipArr);
                } else if (input.startsWith("put ")) {
                    if (!App.getInstance().groupMembershipService.electionInProgress) {
                        String[] putData = input.split(" ");
                        if (putData.length != 3) {
                            System.out.println("Usage: put <Local File Name> <fs533 File Name>");
                            continue;
                        }
                        long start = System.currentTimeMillis();
                        fs533Client.put(putData[1], putData[2]);
                        long end = System.currentTimeMillis();
                        System.out.println("Put Time: " + (end - start) + "ms");
                    } else {
                        System.out.println("Election in Progress. Try again Later !!");
                    }
                } else if (input.startsWith("get ")) {
                    if (!App.getInstance().groupMembershipService.electionInProgress) {
                        String[] getData = input.split(" ");
                        if (getData.length != 3) {
                            System.out.println("Usage: get <fs533 File Name> <Local File Name>");
                            continue;
                        }
                        long start = System.currentTimeMillis();
                        fs533Client.get(getData[1], getData[1]);
                        long end = System.currentTimeMillis();
                        System.out.println("Get Time: " + (end - start) + "ms");
                    } else {
                        System.out.println("Election in Progress. Try again Later !!");
                    }
                } else if (input.startsWith("remove ")) {
                    if (!App.getInstance().groupMembershipService.electionInProgress) {
                        String[] putCommand = input.split(" ");
                        if (putCommand.length != 2) {
                            System.out.println("Usage: delete <fs533_file_name>");
                            continue;
                        }
                        fs533Client.delete(putCommand[1]);
                    } else {
                        System.out.println("Election in Progress. Try again Later !!");
                    }
                } else if ("lshere".equals(input.trim())) {
                    System.out.println("Local File Map: " + fs533Client.fileMap.toString());
                } else if ("pl".equals(input.trim())) {
                    if (!App.getInstance().groupMembershipService.electionInProgress) {
                        System.out.println("Current Leader is: " + App.getInstance().groupMembershipService.getLeader().getIP());
                    } else {
                        System.out.println("Election in Progress. Try again Later !!");
                    }
                } else if ("ls".equals(input.trim())) {
                    if (!App.getInstance().groupMembershipService.electionInProgress) {
                        listFilesFromFS533();
                    } else {
                        System.out.println("Election in Progress. Try again Later !!");
                    }
                } else if (input.startsWith("locate ")) {
                    if (!App.getInstance().groupMembershipService.electionInProgress) {
                        String[] fileName = input.split(" ");
                        if (fileName.length != 2) {
                            System.out.println("Usage: locate <fs533 File Name> ");
                            continue;
                        }
                        locateFile(fileName[1]);
                    } else {
                        System.out.println("Election in Progress. Try again Later !!");
                    }
                }

            } catch (IOException | ParseException e) {
                System.out.println("Exception Occured");
            }
        }
    }

    /**
     * This method will connect to leader node, retrieve list of all files in FS533 and print it in console.
     */
    private void listFilesFromFS533() {
        try {
            ArrayList<String> filesList = new ArrayList<String>();
            if (App.getInstance().groupMembershipService.isLeader()) {
                filesList = App.getInstance().fs533Server.allFiles();
                System.out.println("The files in FS533 ");
                for (String name : filesList) {
                    System.out.println(name);
                }
            } else {
                clientSocket = new Socket(App.getInstance().groupMembershipService.getLeader().getIP(), App.TCP_FS533_PORT);
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                out.println("listFiles:" + "ListFiles");
                String fileName = "";
                System.out.println("The file(s) in FS533 ");
                while (!(fileName = in.readLine()).equals("<END>")) {
                    System.out.println(fileName);
                }
                in.close();
                out.close();
                clientSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method will connect to leader node and retrieve list of nodes in FS533 for a given FS533 key.
     * @param fileName
     */
    private void locateFile(String fileName) {
        try {
            ArrayList<Machines> filesList = new ArrayList<Machines>();
            if (App.getInstance().groupMembershipService.isLeader()) {
                filesList = App.getInstance().fs533Server.locateFile(fileName);
                System.out.println("The File is present in below IP Address ");
                for (Machines machine : filesList) {
                    System.out.println(machine.getIP());
                }
            } else {
                clientSocket = new Socket(App.getInstance().groupMembershipService.getLeader().getIP(), App.TCP_FS533_PORT);
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                out.println("locateFile:" + fileName);
                String fileIP = "";
                System.out.println("The File is present in below IP Address ");
                while (!(fileIP = in.readLine()).equals("<END>")) {
                    System.out.println(fileIP);
                }
                in.close();
                out.close();
                clientSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String[] getStringArray(ArrayList<String> alStr) {
        String[] strArr = new String[alStr.size()];
        for (int i = 0; i < alStr.size(); i++) {
            strArr[i] = alStr.get(i);
        }
        return strArr;
    }

    /**
     * Starts Gateway service, Grep Server and Gossip Failure Detection Services
     */
    public void startServices() {
        groupGateway.start();
        GrepServer.startGrepServer();
        fs533Server.start();
    }

    /**
     * Stop Gateway service
     */
    public void stopGroupServices() {
        if (groupGateway.isAlive()) {
            groupGateway.stopGateway();
            try {
                groupGateway.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
                LOGGER.warning("Could not join GroupServer thread.");
            }
        }
    }

    /**
     * Remove node from the group membership and broadcast to all members
     */
    public void leaveGroup() {
        try {
            String currentHost = InetAddress.getLocalHost().getHostAddress();
            Machines removeNode = new Machines(currentHost, App.UDP_LISTEN_PORT);
            groupGateway.broadcastMessage(removeNode, "R");
            groupMembershipService.nodeList.clear();
            System.out.println("Group membership list cleared on this node.");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Prompt user with input options to leave/join/print and exit.
     */
    private void getUserInput() {
        synchronized (groupMembershipService.nodeList) {
            System.out.println("(l) Leave group member service");
            System.out.println("(j) Join group member service");
            System.out.println("(p) Print Current Membership List");
            System.out.println("(s) Perform Grep Search");
            System.out.println("(put) Add file to fs533");
            System.out.println("(get) Retrieve file from fs533");
            System.out.println("(remove) Delete file from fs533");
            System.out.println("(ls) List File(s) from fs533");
            System.out.println("(locate) List Machines containing file");
            System.out.println("(lshere) List file(s) on local machine");
            System.out.println("(pl) Print Leader Node");
            System.out.println("(e) Exit Application");
        }
    }
}
