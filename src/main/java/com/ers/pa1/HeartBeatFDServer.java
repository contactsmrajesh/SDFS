package com.ers.pa1;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


/**
 * This class implements the failure detector server to process the heart messages
 * in a producer and consumer pattern. The failure server listens to the heartbeats on a UDP
 * port. The constructor kick starts the producer and consumer threads.
 */

public class HeartBeatFDServer {
	protected DatagramSocket socket = null;
	private Thread producer;
	private Thread consumer;
	protected static BlockingQueue<Machines> hbQueue;
	
	//Constructor which starts listening on UDP port and kick-starts the producer and consumer threads
	public HeartBeatFDServer()
	{	
		try {
			socket = new DatagramSocket(App.UDP_HB_PORT);
		} catch (SocketException e1) {
			e1.printStackTrace();
			App.LOGGER.info("HeartBeatFDServer : Unable to start server on port - producer.run() : "+
					App.UDP_HB_PORT);
		}

		hbQueue = new ArrayBlockingQueue<Machines>(30);
		producer = new Thread(new HeartbeatProducer());
		consumer = new Thread(new HeartbeatConsumer());
		producer.start();
		consumer.start();
	}

	/* The Consumer Runnable class implements to process the heartbeats queued by the
	 * producer process.
	 * If the Leader election is not in progress it performs failure detection
	 * Else it continues to add the to be processed heartbeats to a temp processing queue by updating the timestamps
	 * in the nodes its monitoring.
	 */

	public class HeartbeatConsumer implements Runnable
	{
		public HeartbeatConsumer() { }

		/**Method to consume the heartbeat messages
		 * @return
		 */
		private List<Machines> queueProcessing()
		{
			List<Machines> hbToProcess = new ArrayList<Machines>();
			synchronized(hbQueue)
			{
				while(hbQueue.size() > 0)
				{
					Machines temp = hbQueue.remove();
					hbToProcess.add(temp);
				}
			}
			return hbToProcess;
		}

		/**
		 * Method to perform the failure detection and notifies other nodes in the group if failure detected
		 * @param hbToProcess
		 */
		private void failureDetect(List<Machines> hbToProcess)
		{
			long curTime = new Date().getTime();
			synchronized(App.getInstance().groupMembershipService.nodeList)
			{				
				Machines machine = App.getInstance().groupMembershipService.getHeartbeatReceiveNode();
				
				if(machine != null)
				{
					// processing the updates to the heartbeat queue
					int equalsComparisons = 0;
					for(Machines updateMachine : hbToProcess)
					{
						if(updateMachine.equals(machine))
						{
							equalsComparisons++;
							if(updateMachine.lastUpdatedCompareTo(machine) > 0)
							{
								machine.lastUpdatedTimestamp = updateMachine.lastUpdatedTimestamp;
							}
						}
					}
					if(equalsComparisons != hbToProcess.size())
					{
						App.LOGGER.warning("HeartBeatFDServer - heartbeats in the queue and the membership list out of sync - consumer.run()");
					}
					
					// Failure detection is performed here
					if(machine.lastUpdatedTimestamp > 0)
					{
						if((machine.lastUpdatedTimestamp+ App.timeBoundedFailureInMilli) <
								curTime)
						{
							App.LOGGER.info(new Date().getTime()+" HeartBeatFDServer - failure detected at node: "+ machine.verboseToString());

							App.LOGGER.info("Failure detected at node: "+ machine.verboseToString());
							
							try {
								// To notify the other nodes that failed node has been removed
								broadcast(machine, "R");
							} catch (UnknownHostException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}

							App.LOGGER.info("HeartBeatFDServer - removing failed node.");
							App.getInstance().groupMembershipService.removeNodeFromGroup(machine);
							
							if(App.getInstance().groupMembershipService.isLeader())
							{
								App.getInstance().fs533Server.removeFailedNodeEntries(machine);
							}
							
						}
					}
			    }
			}
		}
	
		@Override
		public void run() {
			App.LOGGER.info("HeartBeatFDServer - Consumer thread begins");
			while(!Thread.currentThread().isInterrupted())
			{
				//Check to see if the election is in progress
				boolean electionInProgress = false;
				synchronized(App.getInstance().groupMembershipService.nodeList)
				{
					electionInProgress = App.getInstance().groupMembershipService.electionInProgress;
				}
				//If election is not in progress
				if(!electionInProgress)
				{
					List<Machines> heartbeatsToProcess = queueProcessing();
					failureDetect(heartbeatsToProcess);
				} else {
					queueProcessing();
					App.getInstance().groupMembershipService.resetLastUpdated();
				}
				
				// sleep 2.5 seconds
				long start = new Date().getTime();
				long end = start;
				while((end - start) < 2500)
				{
					try {
						Thread.sleep(2500);
					} catch (InterruptedException e) {
						return;
					}
					end = new Date().getTime();
				}
			}
		}

		/**
		 * Method to notify all nodes telling them to perform an action on this node in their list.
		 *
		 * @param machine Notify others about some change that happened to this node
		 * @param action Could be an Add "A" or Remove "R" event
		 * @throws UnknownHostException
		 * @throws IOException
		 */
		private void broadcast(Machines machine, String action) throws UnknownHostException,
				IOException {
			DatagramSocket socket = new DatagramSocket();
			synchronized (App.getInstance().groupMembershipService.nodeList) {
				for (Machines node : App.getInstance().groupMembershipService.nodeList) {
					byte[] nodeData = new byte[1024];
					String notifyMessage = action + ":" + machine.getIP();
					nodeData = notifyMessage.getBytes();

					App.LOGGER.info("HeartBeatFDServer - broadcastmessage() updating to all nodes in UDP group about members.");
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
	}

	/*
	 * Producer runnable implements to wait on receiving updates over a UDP port.
	 * Once a message is received, it will implements to act according the data message type received( ELECTION, ANSWER, LEADER, HEARTBEAT).
	 * It is responsible to add the update to the heartbeat queue for the consumer to process.
	 */

	public class HeartbeatProducer implements Runnable
	{
		@Override
		public void run() {
			App.LOGGER.info("HeartBeatFDServer - producer thread begins");
			
			while(!Thread.currentThread().isInterrupted())
			{
				
				byte[] buf = new byte[256];
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				try {
					socket.receive(packet);
				} catch (IOException e) {
					App.LOGGER.info("Receiving heartbeats interrupted.");
				}
				
		    	try {
					String data = new String(packet.getData(),0, packet.getLength(), "UTF-8");
					App.LOGGER.info("******The data from machine " + packet.getAddress().toString().replace("/","") + "is : " + data);
					
					if(data.equals("HEARTBEAT"))
					{
						//hearts beats are added to the queue for the consumer to process
						synchronized(hbQueue)
						{
							Machines heartbeatMachine = new Machines(System.currentTimeMillis(),packet.getAddress().toString().replace("/",""),packet.getPort(),System.currentTimeMillis());
							hbQueue.add(heartbeatMachine);
						}
					}
					else if(data.equals("ELECTION"))
					{
						//Election process to identify the node with the largest IP
						App.LOGGER.info("HeartBeatFDServer - producer.run() - received ELECTION message");

						synchronized(App.getInstance().groupMembershipService)
						{
							App.getInstance().groupMembershipService.electionInProgress = true;
						}

						Machines sendingMachine = new Machines(packet.getAddress().getHostAddress(), App.UDP_HB_PORT);
						if(App.verbose)
							App.LOGGER.info("Election message received from: "+ sendingMachine.getIP());
						Machines self = null;
						synchronized(App.getInstance().groupMembershipService)
						{
							self = App.getInstance().groupMembershipService.getSelfNode();
						}
						App.LOGGER.info(" ******comparing self " + self + "  with the sending machine " +sendingMachine+ " : " + self.compareTo(sendingMachine));
						if(self.compareTo(sendingMachine) > 0)
						{
							InetAddress target = null;
							try {
								target = InetAddress.getByName(sendingMachine.getIP());
							} catch (UnknownHostException e) {
								e.printStackTrace();
							}
							
							try {
								sendData(target,"ANSWER");
							} catch (IOException e) {
								e.printStackTrace();
							}
						} else if(self.compareTo(sendingMachine) < 0)
						{
							// Nothing to send
						} else {
							App.LOGGER.warning("HeartBeatFDServer - received a self election message.");
						}
					} else if(data.equals("ANSWER")) {
						HeartBeatFDClient.receivedAnswerMessage(new Date().getTime());
						
						String sendingNodeIP = packet.getAddress().getHostAddress();
						if(App.verbose)
							App.LOGGER.info("Received answer message from: "+sendingNodeIP);
						App.LOGGER.info("HeartBeatFDServer - received answer message from : "+sendingNodeIP);
					} else if(data.equals("LEADER")) {

						// Election of the new leader
						String sendingNodeIP = packet.getAddress().getHostAddress();
						if(App.verbose)
							App.LOGGER.info("Received leader message from: "+sendingNodeIP);
						boolean result = App.getInstance().groupMembershipService.receivedCoordinatorMessage(new Machines(sendingNodeIP, App.UDP_HB_PORT));
						synchronized(HeartBeatFDClient.leaderElection)
						{
							HeartBeatFDClient.resetLeaderElectionStatus();
						}
						if(result)
						{
							App.LOGGER.info("HeartBeatFDServer - received leader message from: "+sendingNodeIP);
						} else {
							App.LOGGER.warning("HeartBeatFDServer - received leader message from node not in list!: "+sendingNodeIP);
						}
					}
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}
		}
		
		
	    private void sendData(InetAddress target, String data) throws IOException{
	    	DatagramSocket datagramSocket = null;
			try {
				datagramSocket = new DatagramSocket();
			} catch (SocketException e) {
				e.printStackTrace();
			}
    		if(!HeartBeatFDClient.isRandomFailure())
    		{
	    		// generate random number to determine whether or  not to drop the packet
		    	if(data.getBytes("utf-8").length > 256) {
		    		App.LOGGER.info("HeartBeatFDServer - String too long to handle");
		    		throw new IOException("String too long to handle");
		    	}
		        DatagramPacket datagram = new DatagramPacket(data.getBytes("utf-8"), data.length(), target, App.UDP_HB_PORT);
		        datagramSocket.send(datagram);
    		} else {
				App.LOGGER.info(new Date().getTime() +" HeartBeatFDServer -  \""+data+"\" message dropped!");
    		}
	    }
		
	}
	
}
