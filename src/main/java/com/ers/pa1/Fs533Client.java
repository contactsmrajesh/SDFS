package com.ers.pa1;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * This client process acts as an interface between FS533 operations(put/get/remove) and the server(leader node).
 * It also perform the replication by transfering file to respective nodes in the group membership service.
 */
public class Fs533Client {

    public Map<String, String> fileMap; // Key: FS533 Path, Value: local path
    private Socket clientSocket = null;

    public Fs533Client() {
        fileMap = new HashMap<String, String>();
    }

    /**
     * This method is responsible for copying the file from local path to FS533 path and vice versa.
     * @param sourceFile
     * @param destFile
     * @throws IOException
     */
    public static void transferFileLocally(File sourceFile, File destFile) throws IOException {
        if (!destFile.exists()) {
            destFile.createNewFile();
        }
        FileInputStream fis = null;
        FileOutputStream fos = null;
        FileChannel sourceChannel = null;
        FileChannel destChanel = null;
        try {
            fis = new FileInputStream(sourceFile);
            sourceChannel = fis.getChannel();
            fos = new FileOutputStream(destFile);
            destChanel = fos.getChannel();
            long transfered = 0;
            long bytes = sourceChannel.size();
            while (transfered < bytes) {
                transfered += destChanel.transferFrom(sourceChannel, 0, sourceChannel.size());
                destChanel.position(transfered);
            }
        } finally {
            if (sourceChannel != null) {
                sourceChannel.close();
            } else if (fis != null) {
                fis.close();
            }
            if (destChanel != null) {
                destChanel.close();
            } else if (fos != null) {
                fos.close();
            }
        }
    }

    /**
     * This method will generate a unique file name in the FS533 path to store the file from local path.
     * @param localFileName
     * @return
     */
    public String randomFileGenerator(String localFileName) {
        String[] parts = localFileName.split("[.]");
        String fileExtension = ".data";
        if (parts.length > 1)
            fileExtension = "." + parts[parts.length - 1];
        return App.FS533_DIR + File.separator + App.hostaddress + "_" + UUID.randomUUID().toString() + fileExtension;
    }

    /**
     * This method retrieves file from FS533 path and copy it to local path.
     * @param fs533FilePath
     * @param localFileName
     */
    public void get(String fs533FilePath, String localFileName) {
        System.out.println("FS533Client Process - starting get");
        if (fileMap.containsKey(fs533FilePath)) {
            System.out.println("Copying file from local cache");
            try {
                transferFileLocally(new File(fileMap.get(fs533FilePath)), new File(localFileName));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            try {
                String ipToPlaceFile = "";
                if (App.getInstance().groupMembershipService.isLeader()) {
                    ipToPlaceFile = App.getInstance().fs533Server.whereis(fs533FilePath);
                } else {
                    clientSocket = new Socket(App.getInstance().groupMembershipService.getLeader().getIP(), App.TCP_FS533_PORT);
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    out.println("whereis:" + fs533FilePath);
                    ipToPlaceFile = in.readLine();
                    in.close();
                    out.close();
                }
                System.out.println("File is present in Network IP " + ipToPlaceFile);

                if (!ipToPlaceFile.equals("")) {
                    clientSocket = new Socket(ipToPlaceFile, App.TCP_FS533_PORT);
                    ServerSocket serverSocket = new ServerSocket(0);

                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                    out.println("get:" + fs533FilePath + ":" + serverSocket.getLocalPort());
                    System.out.println("get:" + fs533FilePath + ":" + serverSocket.getLocalPort());

                    Socket socket = serverSocket.accept();
                    byte[] buffer = new byte[65536];
                    int number;
                    InputStream socketStream = socket.getInputStream();
                    File f = new File(localFileName);
                    OutputStream fileStream = new FileOutputStream(f);
                    while ((number = socketStream.read(buffer)) != -1) {
                        fileStream.write(buffer, 0, number);
                    }
                    socket.close();
                    serverSocket.close();

                    fileStream.close();
                    socketStream.close();
                    clientSocket.close();

                    updateFileMap(fs533FilePath, localFileName);
                } else {
                    System.out.println("File not found.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * This method for a given file, identify replication nodes by connecting to leader node and initiate
     * transfer to the nodes in group membership service.
     * @param fs533Key
     */
    public void putArray(String fs533Key) {
        try {
            System.out.println("The file is placed in FS533 file path in IP " + App.hostaddress);
            ArrayList<Machines> nodesToReplicateFile = new ArrayList<Machines>();
            if (App.getInstance().groupMembershipService.isLeader()) {
                nodesToReplicateFile = App.getInstance().fs533Server.whereputArray(fs533Key, App.hostaddress);
            } else {
                clientSocket = new Socket(App.getInstance().groupMembershipService.getLeader().getIP(), App.TCP_FS533_PORT);
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                out.println("whereputArray:" + fs533Key);
                String nodeToReplicate = "";
                while (!(nodeToReplicate = in.readLine()).equals("<END>")) {
                  //  System.out.println("The Node to replicate file is " + nodeToReplicate);
                    nodesToReplicateFile.add(new Machines(nodeToReplicate, 0));
                }
                in.close();
                out.close();
                clientSocket.close();
            }
            if (!nodesToReplicateFile.isEmpty()) {
                for (Machines machine : nodesToReplicateFile) {
                    System.out.println("The file is replicated in Node " + machine.getIP());
                    Socket socket = new Socket(machine.getIP(), App.TCP_FS533_PORT);
                    ServerSocket serverSocket = new ServerSocket(0);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println("put:" + fs533Key + ":" + serverSocket.getLocalPort());
                    transferFileToNode(serverSocket, fileMap.get(fs533Key));
                    socket.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * This method is invoked when user initiate put request, it copies the file from local path to fs533 path
     * and initiate replication process.
     * @param localFile
     * @param fs533Key
     */
    public void put(String localFile, String fs533Key) {
        String fileName = randomFileGenerator(localFile);
        try {
            transferFileLocally(new File(localFile), new File(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        fileMap.put(fs533Key, fileName);
        putArray(fs533Key);
    }

    /**
     * Remove file from fs533 file system. Retrive list of nodes where the file is present from leader node list
     * and iteratively removes the file from all the nodes.
     * @param fs533Key Key of file to remove
     */
    public void delete(String fs533Key) {
        if (fileMap.containsKey(fs533Key)) {
            deleteFS533LocalFile(fs533Key);
        }
        try {
            ArrayList<Machines> nodesToRemoveFileFrom = new ArrayList<Machines>();
            if (App.getInstance().groupMembershipService.isLeader()) {
                nodesToRemoveFileFrom = App.getInstance().fs533Server.wheredelete(fs533Key,
                        App.getInstance().groupMembershipService.getSelfNode().getIP());
            } else {
                clientSocket = new Socket(App.getInstance().groupMembershipService.getLeader().getIP(), App.TCP_FS533_PORT);
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                out.println("wheredelete:" + fs533Key);
                String nodeToDeleteFrom = "";
                while (!(nodeToDeleteFrom = in.readLine()).equals("<END>")) {
                    System.out.println("The Node to delete file from is " + nodeToDeleteFrom);
                    nodesToRemoveFileFrom.add(new Machines(nodeToDeleteFrom, 0));
                }
                in.close();
                out.close();
                clientSocket.close();
            }
            for (Machines machine : nodesToRemoveFileFrom) {
                clientSocket = new Socket(machine.getIP(),
                        App.TCP_FS533_PORT);

                // Setup our input and output streams
                PrintWriter out = new PrintWriter(
                        clientSocket.getOutputStream(), true);
                out.println("delete:" + fs533Key);
            }
        } catch (Exception e) {
            System.out.println("DELETE: Fail");
            e.printStackTrace();
        }
    }

    /**
     * Deletes file from local disk and file map
     * @param fs533Key
     */
    public void deleteFS533LocalFile(String fs533Key) {
        String localFilePath = fileMap.get(fs533Key);
        File fileToDelete = new File(localFilePath);
        System.out.println("Deleting local file: " + localFilePath);
        if (fileToDelete.delete()) {
            System.out.println("File Deleted.");
        } else {
            System.out.println("Failed to remove file from disk. Will only remove from file map.");
        }
        fileMap.remove(fs533Key);
    }

    public synchronized void updateFileMap(String fs533Key, String localPath) {
        fileMap.put(fs533Key, localPath);
    }

    public synchronized Set<String> getFilesOnNode() {
        return fileMap.keySet();
    }

    public synchronized boolean hasFile(String fs533Key) {
        return fileMap.containsKey(fs533Key);
    }

    public synchronized String getFileLocation(String fs533Key) {
        return fileMap.get(fs533Key);
    }

    /**
     * This method is used to replicate fs533 file to nodes in the group membership service.
     * @param socket
     * @param localFileName
     * @return
     */
    private String transferFileToNode(ServerSocket socket, String localFileName) {
        byte[] buffer = new byte[65536];
        int number;
        OutputStream socketOutputStream = null;
        FileInputStream fileInputStream = null;

        try {
            Socket clientSocket = socket.accept();
            socketOutputStream = clientSocket.getOutputStream();
            fileInputStream = new FileInputStream(localFileName);
            while ((number = fileInputStream.read(buffer)) != -1) {
                try {
                    socketOutputStream.write(buffer, 0, number);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            socketOutputStream.close();
            fileInputStream.close();
            socket.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        return null;
    }
}
