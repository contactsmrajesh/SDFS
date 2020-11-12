package com.ers.pa1;

import java.io.IOException;
import java.net.*;

/**
 * This program implements the core features of Group Membership Service. Below are the list of features
 * implemented.
 * 1. New Node join and add it to the group member list, 2. Send the current list of nodes to the new joining
 * node. 3. Broadcast the new node join details to the group membership list. 4. Broadcast when a node leave
 * to the group membership list.
 */
public class GroupMemberGateway extends Thread {
    public boolean groupMember = false;
    protected DatagramSocket socket = null;
    protected volatile boolean execute = true;
    // private HearbeatClient hearbeatClient;
    public boolean joinedGroup = false;


    /**
     * Stop gateway service and close socket connection
     */
    public void stopGateway() {
        execute = false;
        super.interrupt();
        socket.close();
    }

    /**
     * Open socket connection and listen for join/leave requests from participating nodes.
     * Based on the action either join or remove node from group membership list.
     */
    @Override
    public void run() {
        try {
            socket = new DatagramSocket(App.UDP_LISTEN_PORT);
        } catch (SocketException e1) {
            App.LOGGER.severe("GroupMemberGateway - Watch out!Socket already occupied: " + App.UDP_LISTEN_PORT);
            return;
        }

        boolean groupList = true;
        while (execute) {
            try {
                byte[] buf = new byte[1024];
                Machines nodeRemoveIP = null;
                String action = "";

                App.LOGGER.info("GroupMemberGateway - run() - Join request recieved");
                //joinedGroup = true;

                if (App.hostaddress.equals(App.GATEWAYNODE_IP)) {
                    //joinedGroup = true;
                    DatagramPacket addNodePacket = new DatagramPacket(buf, buf.length);
                    socket.receive(addNodePacket);
                    System.out.println("Received join request from " + addNodePacket.getAddress().getHostAddress());
                    String groupMemberGatewayMsg = new String(
                            addNodePacket.getData(),
                            0,
                            addNodePacket.getLength());
                    if (groupMemberGatewayMsg.contains(":")) {
                        nodeRemoveIP = parseNodeFromMessage(groupMemberGatewayMsg);
                        action = parseActionFromMessage(groupMemberGatewayMsg);
                    }
                    Machines addNode = new Machines(addNodePacket.getAddress().getHostAddress(), App.UDP_LISTEN_PORT);
                    synchronized (App.getInstance().groupMembershipService.nodeList) {
                        if ((!App.getInstance().groupMembershipService.nodeList.contains(addNode))) {
                            addNodeToList(addNode);
                            System.out.println("Updated Group Membership List "
                                    + App.getInstance().groupMembershipService.toString());
                            joinedGroup = true;

                        }

                        if ("R".equals(action)) {
                            App.getInstance().groupMembershipService.removeNodeFromGroup(nodeRemoveIP);
                            System.out.println("Updated Group Membership List "
                                    + App.getInstance().groupMembershipService.toString());
                        }
                    }
                } else {
                    DatagramPacket dataPacket = new DatagramPacket(buf, buf.length);
                    //joinedGroup = true;
                    socket.receive(dataPacket);
                    String groupMemberGatewayMsg = new String(
                            dataPacket.getData(),
                            0,
                            dataPacket.getLength());

                    synchronized (App.getInstance().groupMembershipService.nodeList) {
                        if (groupMemberGatewayMsg.equalsIgnoreCase("END")) {
                            groupList = false;
                        } else {
                            Machines nodeInfo = parseNodeFromMessage(groupMemberGatewayMsg);
                            String groupAction = parseActionFromMessage(groupMemberGatewayMsg);
                            if ("A".equals(groupAction)
                                    && (!App.getInstance().groupMembershipService.nodeList.contains(nodeInfo))) {
                                App.getInstance().groupMembershipService.addNodeToList(nodeInfo);
                                System.out.println("Updated Group Membership List "
                                        + App.getInstance().groupMembershipService.toString());
                                joinedGroup = true;

                            } else if ("R".equals(groupAction)
                            ) {
                                App.getInstance().groupMembershipService.removeNodeFromGroup(nodeInfo);
                                System.out.println("Updated Group Membership List "
                                        + App.getInstance().groupMembershipService.toString());
                            }
                        }
                    }
                }

            } catch (IOException e) {
                App.LOGGER.severe("GroupMemberGateway IO Exception");
                execute = false;
            }
        }
        //System.out.println("Socket closed");

        socket.close();
    }

    /**
     * Get the Node IP from the packet message.
     *
     * @param addGroupMemberMessage
     * @return IP of the node to join/remove
     */
    private Machines parseNodeFromMessage(String addGroupMemberMessage) {
        String[] parts = addGroupMemberMessage.split(":");
        if (parts.length != 2) {
            return null;
        }
        return new Machines(parts[1], App.UDP_LISTEN_PORT);
    }

    /**
     * Get information of the Action from the packet message
     *
     * @param addGroupMemberMessage
     * @return Action to be performed (Add or Remove)
     */
    private String parseActionFromMessage(String addGroupMemberMessage) {
        String[] parts = addGroupMemberMessage.split(":");
        if (parts.length != 2) {
            return null;
        }
        return parts[0];
    }

    /**
     * Invoke group membership service add node functionality to update the list
     *
     * @param newNode
     */
    private void addNodeToList(Machines newNode) throws IOException {
        String groupList;
        synchronized (App.getInstance().groupMembershipService) {
            sendGroupListToNode(newNode);
            App.getInstance().groupMembershipService.addNodeToList(newNode);
            groupList = App.getInstance().groupMembershipService.toString();
        }
        App.LOGGER.info("GroupMemberGateway- addNodeToList - New node added successfully: " + newNode);
        try {
            broadcastMessage(newNode, "A");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send the current group membership list to the joining node
     *
     * @param newNode
     * @throws IOException
     */
    private void sendGroupListToNode(Machines newNode) throws IOException {
        byte[] buf;
        InetAddress nodeAddress = InetAddress.getByName(newNode.getIP());
        String nodeMessage = "";
        synchronized (App.getInstance().groupMembershipService.nodeList) {
            for (Machines node : App.getInstance().groupMembershipService.nodeList) {
                nodeMessage = "A:" + node.getIP();
                buf = nodeMessage.getBytes();
                DatagramPacket addNodePacket = new DatagramPacket(buf, buf.length, nodeAddress,
                        App.UDP_LISTEN_PORT);
                socket.send(addNodePacket);

            }
        }
    }

    /**
     * Broadcast the join/leave node details to the participating nodes in the group membership list
     *
     * @param newNode
     * @param action
     * @throws UnknownHostException
     * @throws IOException
     */
    public void broadcastMessage(Machines newNode, String action) throws UnknownHostException,
            IOException {
        DatagramSocket socket = new DatagramSocket();
        synchronized (App.getInstance().groupMembershipService.nodeList) {
            for (Machines node : App.getInstance().groupMembershipService.nodeList) {
                byte[] nodeData = new byte[1024];
                String notifyMessage = action + ":" + newNode.getIP();
                nodeData = notifyMessage.getBytes();

                App.LOGGER.info("GroupMemberGateway - broadcastmessage() updating to all nodes in UDP group about members.");
                InetAddress nodeAddress = InetAddress.getByName(node.getIP());
                if ((!App.GATEWAYNODE_IP.equals(nodeAddress.getHostAddress())) && "A".equals(action)) {
                    DatagramPacket addNodePacket = new DatagramPacket(nodeData, nodeData.length, nodeAddress,
                            App.UDP_LISTEN_PORT);
                    socket.send(addNodePacket);
                } else if ("R".equals(action)) {
                    DatagramPacket addNodePacket = new DatagramPacket(nodeData, nodeData.length, nodeAddress,
                            App.UDP_LISTEN_PORT);
                    socket.send(addNodePacket);
                }
            }
        }
    }

    /**
     * Invoked if user wants the node to join the group membership list
     */
    public void joinGroupMemberService() {
        //attemptingJoin = true;
        boolean result = initiateJoin();
    }

    /**
     * Initiate the join request by sending the node information to the gateway node.
     * The gateway node will update the group membership list and broadcast to participating nodes
     */

    private boolean initiateJoin() {
        String initiateRequest = String.valueOf(System.currentTimeMillis());
        InetAddress address = null;
        try {
            address = InetAddress.getByName(App.GATEWAYNODE_IP);
            App.LOGGER.info("GroupMemberGateway receiving Join request");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        try {
            sendJoinMessage(address, initiateRequest);
            //startHearbeatClient();
        } catch (IOException e) {
            e.printStackTrace();
            App.LOGGER.warning("GroupMemberGateway - Membership Join request from the Client failed");
            return false;
        }
        return true;
    }

    //public void startHearbeatClient() throws IOException {
        //HearbeatClient.startClient();
    //}

    /**
     * Trigger node join message to the gateway node
     *
     * @param target
     * @param data
     * @throws IOException
     */
    private void sendJoinMessage(InetAddress target, String data) throws IOException {
        DatagramSocket socket = new DatagramSocket();
        byte[] nodeData = new byte[1024];
        nodeData = data.getBytes("UTF-8");
        DatagramPacket addNodePacket = new DatagramPacket(nodeData, nodeData.length, target,
                App.UDP_LISTEN_PORT);
        socket.send(addNodePacket);
        socket.close();
    }
}


