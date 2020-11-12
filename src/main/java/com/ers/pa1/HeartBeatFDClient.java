package com.ers.pa1;

import java.io.IOException;
import java.net.*;
import java.util.Date;
import java.util.List;

/**
 * This class implements the failure Detector Client which uses the Bully Election Algorithm
 * to elect a leader in case of the leader node failure .
 */

public class HeartBeatFDClient {

	public DatagramSocket socket = null;
	private String hostaddress = "";
	public static double msgFailureRate;
	public static LeaderElection leaderElection = new LeaderElection();
	private Thread client;

	//Constructor which kick-starts the Failure detection Client Thread
	public HeartBeatFDClient() {
		try {
			hostaddress = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		try {
			socket = new DatagramSocket();
		} catch (SocketException e) {
			e.printStackTrace();
		}
		resetLeaderElectionStatus();

		client = new Thread(new HeartBeatFDClientRunnable(),"HeartBeatFDClient");
		client.start();
	}

	//Initializing the Leader election Variables
	public static class LeaderElection {

		private boolean electionMsgsent = false;
		private long timeElectionMsgsent = 0;
		private boolean receivedAnswerMsg = false;
		private long timereceivedAnswerMsg = 0;
	}

	/* Method to reset the Leader election status the first time a new election process and
	 * every time a leader is elected i.e the election completes
	 */
	public static synchronized void resetLeaderElectionStatus()
	{
		HeartBeatFDClient.leaderElection.electionMsgsent = false;
		HeartBeatFDClient.leaderElection.timeElectionMsgsent = 0;
		HeartBeatFDClient.leaderElection.receivedAnswerMsg = false;
		HeartBeatFDClient.leaderElection.timereceivedAnswerMsg = 0;
	}

	/**
	 * Method to track the ANSWER message received during the Election
	 * @param timestamp
	 */
	public static synchronized void receivedAnswerMessage(long timestamp)
	{
		synchronized(HeartBeatFDClient.leaderElection)
		{
			HeartBeatFDClient.leaderElection.receivedAnswerMsg = true;
			HeartBeatFDClient.leaderElection.timereceivedAnswerMsg = timestamp;
		}
	}

	//Check if any Random Failure is encountered
	public static boolean isRandomFailure()
	{
		if(msgFailureRate == 0.0)
		{
			return false;
		} else {
			double rand = Math.random();
			App.LOGGER.info("rand: "+rand);
			if(Math.random() < msgFailureRate)
			{
				return true;
			} else {
				return false;
			}
		}
	}

	/*
	 * The Runnable class implements the core of the Leader election Client process which sends the following messages to
	 * the server as suitable.
	 * “ELECTION” – Message sent to announce election
	 * “ANSWER” – Message sent as a response to the Election message
	 * “LEADER” – Sent by the node that wins the election i.e the node in the group with the largest IP will be elected the leader
	 * “HEARTBEAT” – Message sent to the peers to communicate they are alive periodically.
	 */

	public class HeartBeatFDClientRunnable implements Runnable
	{

		public HeartBeatFDClientRunnable() { }

		public void run()
		{
			while(!Thread.currentThread().isInterrupted()) {

				synchronized(App.getInstance().groupMembershipService.nodeList)
				{
					// No heart beats or communication is required to be exchanged
					if(App.getInstance().groupMembershipService.nodeList.size() < 2)
					{
						continue;
					}
				}
				
				boolean electionInProgress = false;
				synchronized(App.getInstance().groupMembershipService.nodeList)
				{
					electionInProgress = App.getInstance().groupMembershipService.electionInProgress;
					App.LOGGER.info(" ******Is the election in progress for this node in the list" + App.getInstance().groupMembershipService.nodeList + "?" + electionInProgress);
				}

				App.LOGGER.info("******Checking the election status : " + electionInProgress + " & the join group status : " + App.getInstance().groupGateway.joinedGroup );
				if(electionInProgress && App.getInstance().groupGateway.joinedGroup)
				{
					boolean sendElection = false;
					boolean sendLeader = false;
					synchronized(HeartBeatFDClient.leaderElection)
					{
						if(!HeartBeatFDClient.leaderElection.electionMsgsent)
						{
							sendElection = true;
							
						} else {
							if(HeartBeatFDClient.leaderElection.receivedAnswerMsg)
							{
								long timeSinceReceived = new Date().getTime() - HeartBeatFDClient.leaderElection.timereceivedAnswerMsg;
								
								if(timeSinceReceived > (5 * App.timeBoundedFailureInMilli))
								{
									App.LOGGER.warning("HeartBeatFDClient - has not received leader message to coordinate in over 5 * timeout!!");
								}
							} else {
								long timeSinceReceived = new Date().getTime() - HeartBeatFDClient.leaderElection.timeElectionMsgsent;
								if(timeSinceReceived > (2 * App.timeBoundedFailureInMilli))
								{
									App.LOGGER.warning("HeartBeatFDClient - Wohoo! I lead");
									if(App.verbose)
										App.LOGGER.info("Determined I'm the leader");
									sendLeader = true;
								}
							}
						}
					}
					
					if(sendElection && sendLeader)
					{
						App.LOGGER.warning("both leader and election message is being communicated");
					}
					if(sendElection)
					{
						// Sending election message to all the other members / nodes
						List<Machines> otherMachines = App.getInstance().groupMembershipService.getOtherNodes();
						for(int nodeIndex = 0; nodeIndex < otherMachines.size(); nodeIndex++)
						{
							Machines machine = otherMachines.get(nodeIndex);
							InetAddress target = null;
							try {
								target = InetAddress.getByName(machine.getIP());
							} catch (UnknownHostException e) {
								e.printStackTrace();
							}
							
							try {
								sendData(target,"ELECTION");
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
						synchronized(HeartBeatFDClient.leaderElection)
						{
							HeartBeatFDClient.leaderElection.electionMsgsent = true;
							HeartBeatFDClient.leaderElection.timeElectionMsgsent = new Date().getTime();
						}
					}
					if(sendLeader)
					{
						//Sending Leader message to the other lower members in the membership list
						List<Machines> lowerMachines = App.getInstance().groupMembershipService.getLowerMembers();
						for(int nodeIndex = 0; nodeIndex < lowerMachines.size(); nodeIndex++)
						{
							Machines machine = lowerMachines.get(nodeIndex);
							InetAddress target = null;
							try {
								target = InetAddress.getByName(machine.getIP());
							} catch (UnknownHostException e) {
								e.printStackTrace();
							}
							
							try {
								sendData(target,"LEADER");
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
						
						synchronized(App.getInstance().groupMembershipService)
						{
							App.getInstance().groupMembershipService.setSelfAsLeader();
						}
						synchronized(HeartBeatFDClient.leaderElection)
						{
							HeartBeatFDClient.resetLeaderElectionStatus();
						}
						App.getInstance().fs533Server.populateGlobalFileMap();

					}
				} else if(App.getInstance().groupGateway.joinedGroup) {
					// If the election is not in progress only then send heart beats
					int nodesContacted = 0;
					Machines machine = null;
					
					synchronized(App.getInstance().groupMembershipService.nodeList)
					{	
						machine = App.getInstance().groupMembershipService.getHeartbeatSendNode();
					}
					
					if(machine != null)
					{
						nodesContacted++;				
						
						InetAddress target = null;
						try {
							target = InetAddress.getByName(machine.getIP());
						} catch (UnknownHostException e) {
							e.printStackTrace();
						}
						
						try {
							if(App.verbose)
								App.LOGGER.info("Sending the heart beats to:"+ machine.getIP());
							sendData(target,"HEARTBEAT");
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

					if(nodesContacted != 1) {
						App.LOGGER.info(new Date().getTime()+" HeartBeatFDClient - failed to send heartbeat message to peer");
					}
				}

				long start = new Date().getTime();
				long end = start;
				while((end - start) < 2500 && !Thread.currentThread().isInterrupted())
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
		
	    private void sendData(InetAddress target, String data) throws IOException{
    		if(!isRandomFailure())
    		{
	    		// generating a random number to determine whether or  not to drop the packet
		    	if(data.getBytes("utf-8").length > 256) {
		    		App.LOGGER.info("HeartBeatFDClient - String too long to handle");
		    		throw new IOException("String too long to handle");
		    	}
		        DatagramPacket datagram = new DatagramPacket(data.getBytes("utf-8"), data.length(), target, App.UDP_HB_PORT);
		        socket.send(datagram);
    		} else {
				App.LOGGER.info(new Date().getTime() +" HeartBeatFDClient -  \""+data+"\" message dropped!");
    		}
	    }
	}
}
