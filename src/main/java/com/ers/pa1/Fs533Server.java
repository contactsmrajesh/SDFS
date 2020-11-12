package com.ers.pa1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * This is core FS533 server process, running in the leader node of the Group membership service. It receives
 * user requests from client and perform required operations(put/get/delete/list).
 */
public class Fs533Server extends Thread {

    public Map<String, Set<Machines>> fs533FileMap;
    private ServerSocket serverSocket = null;
    private int serverPort = App.TCP_FS533_PORT;

    public Fs533Server() {
        try {
            serverSocket = new ServerSocket(serverPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
        fs533FileMap = new HashMap<String, Set<Machines>>();
    }

    /**
     * This thread will accept client request and invoke appropriate operations based on the request.The main
     * operations are, lookup for a fs533 key and 1 node, get request, put request, lookup to delete, delete request,
     * list files and locate specific files in all nodes.
     */
    @Override
    public void run() {
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String inputLine = in.readLine();
                if (inputLine == null || inputLine.split(":").length < 2) {
                    continue;
                }
                String command = inputLine.split(":")[0];
                String fs533Key = inputLine.split(":")[1];
                if (command.equals("whereis")) {
                    String nodeIp = whereis(fs533Key);
                    out.println(nodeIp);
                    System.out.println("Found FS533 Key:" + fs533Key + ") : " + nodeIp);
                } else if (command.equals("get")) {
                    int clientPort = Integer.parseInt(inputLine.split(":")[2]);
                    new Fs533GetFileHelper(clientSocket.getInetAddress().getHostAddress(), clientPort, fs533Key).start();
                } else if (command.equals("whereputArray")) {
                    ArrayList<Machines> nodesWithFile = whereputArray(fs533Key, clientSocket.getInetAddress().getHostAddress());
                    for (Machines machine : nodesWithFile) {
                        out.println(machine.getIP());
                    }
                    out.println("<END>");
                } else if (command.equals("put")) {
                    int clientPort = Integer.parseInt(inputLine.split(":")[2]);
                    new Fs533PutFileHelper(clientSocket.getInetAddress().getHostAddress(), clientPort, fs533Key).start();
                } else if (command.equals("wheredelete")) {
                    ArrayList<Machines> nodesWithFile = wheredelete(fs533Key, clientSocket.getInetAddress().getHostAddress());
                    for (Machines machine : nodesWithFile) {
                        out.println(machine.getIP());
                    }
                    out.println("<END>");
                    synchronized (fs533FileMap) {
                        fs533FileMap.remove(fs533Key);
                    }
                } else if (command.equals("delete")) {
                    System.out.println("Deleting Key: " + fs533Key);
                    App.getInstance().fs533Client.deleteFS533LocalFile(fs533Key);
                } else if (command.equals("getlist")) {
                    Set<String> localFs533Keys = getList();
                    for (String key : localFs533Keys) {
                        out.println(key);
                    }
                    out.println("<END>");
                } else if (command.equals("replicate")) {
                    App.getInstance().fs533Client.putArray(fs533Key);
                } else if (command.equals("listFiles")) {
                    ArrayList<String> nodesWithFile = allFiles();
                    for (String file : nodesWithFile) {
                        out.println(file);
                    }
                    out.println("<END>");
                } else if (command.equals("locateFile")) {
                    ArrayList<Machines> nodesWithFile = locateFile(fs533Key);
                    for (Machines machine : nodesWithFile) {
                        out.println(machine.getIP());
                    }
                    out.println("<END>");
                }


            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Identify one node where a given fs533key is present. It is used to process the get request.
     * @param fs533Key
     * @return
     */
    public String whereis(String fs533Key) {
        synchronized (fs533FileMap) {
            if (fs533FileMap.containsKey(fs533Key) &&
                    fs533FileMap.get(fs533Key).size() > 0) {
                Iterator<Machines> it = fs533FileMap.get(fs533Key).iterator();
                Machines machine = it.next();
                return machine.getIP();
            }
        }
        return "";
    }

    /**
     * This method returns a list of nodes from the group membership service to replicate a file in FS533.
     * @param fs533Key
     * @param puttingNodeIP
     * @return
     */
    public ArrayList<Machines> whereputArray(String fs533Key, String puttingNodeIP) {
        ArrayList<Machines> result = new ArrayList<Machines>();
        int resultCount = 0;
        synchronized (fs533FileMap) {
            if (fs533FileMap.containsKey(fs533Key)) {
                if (fs533FileMap.get(fs533Key).size() == 3) {
                    System.out.println("Key is already stored at three nodes");
                    return result;
                }
                Set<Machines> machines = fs533FileMap.get(fs533Key);
                machines.add(new Machines(puttingNodeIP, App.TCP_FS533_PORT));
                fs533FileMap.put(fs533Key, machines);
            } else {
                Set<Machines> machines = new HashSet<Machines>();
                machines.add(new Machines(puttingNodeIP, App.TCP_FS533_PORT));
                fs533FileMap.put(fs533Key, machines);
            }
        }
        String ipToPut = "";
        synchronized (App.getInstance().groupMembershipService.nodeList) {
            for (Machines machine : App.getInstance().groupMembershipService.nodeList) {
                if ((!machine.getIP().equals(puttingNodeIP)) && resultCount < 2) {
                    result.add(machine);
                    resultCount++;
                }
            }
        }
        synchronized (fs533FileMap) {
            Set<Machines> machines = fs533FileMap.get(fs533Key);
            for (Machines res : result) {
                machines.add(res);
            }
            fs533FileMap.put(fs533Key, machines);
        }
        return result;
    }

    /**
     * This method will identify list of nodes to perform remove operation.
     * @param fs533Key
     * @param ip
     * @return
     */
    public ArrayList<Machines> wheredelete(String fs533Key, String ip) {
        ArrayList<Machines> result = new ArrayList<Machines>();
        synchronized (fs533FileMap) {
            for (Machines machine : fs533FileMap.get(fs533Key)) {
                if (!machine.getIP().equals(ip)) {
                    result.add(machine);
                }
            }
        }
        return result;
    }

    /**
     * This method will identify list of all nodes where the given fs533 file is present
     * @param fs533Key
     * @return
     */
    public ArrayList<Machines> locateFile(String fs533Key) {
        ArrayList<Machines> result = new ArrayList<Machines>();
        synchronized (fs533FileMap) {
            for (Machines machine : fs533FileMap.get(fs533Key)) {
                result.add(machine);
            }
        }
        return result;
    }

    public ArrayList<String> allFiles() {
        ArrayList<String> result = new ArrayList<String>();
        synchronized (fs533FileMap) {
            for (Map.Entry<String, Set<Machines>> entry : fs533FileMap.entrySet()) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    public String get(String fs533Key) {
        synchronized (App.getInstance().fs533Client) {
            return App.getInstance().fs533Client.getFileLocation(fs533Key);
        }
    }

    public Set<String> getList() {
        synchronized (App.getInstance().fs533Client) {
            return App.getInstance().fs533Client.getFilesOnNode();
        }
    }

    /**
     * This method is invoked when a new leader is elected, it will send the list of FS533 file map to the
     * newly elected leader node.
     */
    void populateGlobalFileMap() {
        long start = new Date().getTime();
        Set<String> localFiles;
        synchronized (App.getInstance().fs533Client) {
            localFiles = App.getInstance().fs533Client.getFilesOnNode();
        }
        for (String fs533Key : localFiles) {
            Set<Machines> machines = new HashSet<Machines>();
            machines.add(App.getInstance().groupMembershipService.getSelfNode());
            synchronized (fs533FileMap) {
                if (!fs533FileMap.containsKey(fs533Key)) {
                    fs533FileMap.put(fs533Key, machines);
                } else {
                    boolean alreadyAdded = false;
                    Set<Machines> currentMachines = fs533FileMap.get(fs533Key);
                    for (Machines machine : currentMachines) {
                        if (machine.getIP().equals(App.hostaddress)) {
                            alreadyAdded = true;
                        }
                    }
                    if (!alreadyAdded) {
                        currentMachines.add(App.getInstance().groupMembershipService.getSelfNode());
                        fs533FileMap.put(fs533Key, currentMachines);
                    }
                }
            }
        }

        try {
            List<Machines> allOtherMachines = App.getInstance().groupMembershipService
                    .getOtherNodes();
            for (Machines machine : allOtherMachines) {
                App.getInstance().groupMembershipService.getLeader().getIP();

                Socket cSocket = new Socket(machine.getIP(),
                        App.TCP_FS533_PORT);
                PrintWriter out = new PrintWriter(cSocket.getOutputStream(),
                        true);
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        cSocket.getInputStream()));
                out.println("getlist:FILLERDATA");
                String inputLine = "";

                while (!(inputLine = in.readLine()).equals("<END>")) {
                    String currFs533Key = inputLine;
                    synchronized (fs533FileMap) {
                        if (!fs533FileMap.containsKey(currFs533Key)) {
                            Set<Machines> machines = new HashSet<Machines>();
                            machines.add(machine);
                            fs533FileMap.put(currFs533Key, machines);
                        } else {
                            Set<Machines> machines = fs533FileMap.get(currFs533Key);
                            boolean foundNode = false;
                            for (Machines currMachine : machines) {
                                if (currMachine.getIP().equals(machine.getIP())) {
                                    foundNode = true;
                                }
                            }
                            if (!foundNode) {
                                machines.add(machine);
                                fs533FileMap.put(currFs533Key, machines);
                            }
                        }
                    }
                }
                out.close();
                in.close();
                cSocket.close();

                searchForReplication(true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        long time = new Date().getTime() - start;
        //System.out.println("Populated FS533 file map took " + time + "ms");
    }

    /**
     * This method is invoked to perform replication when a new leader node is elected or during failure of an
     * existing node.
     * @param doPut
     * @throws IOException
     */
    public void searchForReplication(boolean doPut) throws IOException {
        Map<String, String> ipMessagePairs = new HashMap<String, String>();
        synchronized (fs533FileMap) {
            for (String key : fs533FileMap.keySet()) {
                if (fs533FileMap.get(key).size() == 0) {
                    fs533FileMap.remove(key);
                } else if (fs533FileMap.get(key).size() == 1) {
                    Iterator<Machines> it = fs533FileMap.get(key).iterator();
                    Machines machine = it.next();
                    System.out.println("Replicating Key" + key);
                    if (doPut) {
                        ipMessagePairs.put(key, machine.getIP());
                    }
                } else if (fs533FileMap.get(key).size() > 2) {
                    System.out.println("ERROR - FS533 FileMap Entry has more than 2 copies");
                }
            }
        }

        for (String key : ipMessagePairs.keySet()) {
            if (!App.getInstance().groupMembershipService.getSelfNode().getIP().equals(ipMessagePairs.get(key))) {
                Socket cSocket = new Socket(ipMessagePairs.get(key), App.TCP_FS533_PORT);
                PrintWriter out = new PrintWriter(cSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(cSocket.getInputStream()));
                out.println("replicate:" + key);
                out.close();
                in.close();
                cSocket.close();
            } else {
                App.getInstance().fs533Client.putArray(key);
            }

        }
    }

    /**
     * This method is invoked by HeartBeatServer when a node failure is detected. Once a failure is detected, it
     * will invoke replication process to replicate the files in failure node to other available nodes.
     * @param machineToRemove
     */
    public void removeFailedNodeEntries(Machines machineToRemove) {
        try {
            synchronized (fs533FileMap) {
                for (String key : fs533FileMap.keySet()) {
                    Iterator<Machines> it = fs533FileMap.get(key).iterator();
                    while (it.hasNext()) {
                        Machines machine = it.next();
                        if (machine.getIP().equals(machineToRemove.getIP())) {
                            it.remove();
                        }
                    }
                }
            }
            if (App.getInstance().groupMembershipService.nodeList.size() > 1) {
                searchForReplication(true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
