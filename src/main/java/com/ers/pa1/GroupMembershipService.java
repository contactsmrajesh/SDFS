package com.ers.pa1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This program runs in each node and hold the list of nodes in the Group Membership List
 * supporting required node functions.
 */

public class GroupMembershipService {

    public List<Machines> nodeList;
    public boolean electionInProgress = true;
    private int selfIndex = 0;
    private String selfIP;
    private String leaderIP = "";
    private int leaderIndex = 0;


    public GroupMembershipService(String _selfIP) {
        this.nodeList = Collections
                .synchronizedList(new ArrayList<Machines>());
        this.selfIP = _selfIP;
    }

    //resets the last updated timestamp
    public void resetLastUpdated() {
        for (int i = 0; i < this.nodeList.size(); i++) {
            this.nodeList.get(i).lastUpdatedTimestamp = -1;
        }
    }

    /**
     * Method to get the nodes that are ranked lower than the current node
     * @return
     */
    public List<Machines> getLowerMembers() {
        List<Machines> list = new ArrayList<Machines>();
        if (this.nodeList.size() < 2) {
            return list;
        }

        for (int i = 0; i < this.selfIndex; i++) {
            if (i != this.selfIndex) {
                list.add(this.nodeList.get(i));
            }
        }
        return list;
    }

    public Machines getLeader() {
        if (!this.electionInProgress && leaderIndex < this.nodeList.size()) {
            return this.nodeList.get(leaderIndex);
        }
        return null;

    }

    public boolean isLeader() {
		/*if(Application.hostaddress.equals(Application.INTRODUCER_IP))
		{
			return true;
		}
		return false;
		*/
        if (!this.electionInProgress &&
                this.getSelfIndex() == this.leaderIndex) {
            return true;
        }
        return false;
    }

    public void setLeader(String IP) {
        this.leaderIP = IP;

    }

    public void setSelfAsLeader() {
        this.leaderIndex = this.selfIndex;
        this.electionInProgress = false;
        this.leaderIP = App.hostaddress;
    }

    /**
     * return a list of all machines except self
     *
     * @return List<Machines> - all other nodes
     */
    public List<Machines> getOtherNodes() {
        List<Machines> list = new ArrayList<Machines>();
        if (this.nodeList.size() < 2) {
            return list;
        }

        for (int i = 0; i < this.nodeList.size(); i++) {
            if (i != this.selfIndex) {
                list.add(this.nodeList.get(i));
            }
        }
        return list;
    }

    // Once the Leader message is received and leader is elected
    public boolean receivedCoordinatorMessage(Machines machine) {
        for (int i = 0; i < this.nodeList.size(); i++) {
            if (machine.getIP().equals(this.nodeList.get(i).getIP())) {
                this.leaderIndex = i;
                this.electionInProgress = false;
                setLeader(machine.getIP());
                return true;
            }
        }
        return false;
    }

    /**
     * To get the node to send heart beat messages to
     * @return
     */
    public Machines getHeartbeatSendNode() {
        if (this.nodeList.size() < 2) {
            return null;
        }

        if (this.selfIndex == 0) {
            return this.nodeList.get(this.nodeList.size() - 1); // return the last node
        }
        return this.nodeList.get(this.selfIndex - 1);
    }

    /**
     * To get the node to receive heartbeat messages from
     * @return
     */
    public Machines getHeartbeatReceiveNode() {
        if (this.nodeList.size() < 2) {
            return null;
        }

        return this.nodeList.get((this.selfIndex + 1) % this.nodeList.size());
    }

    /**
     * To get the index of the node representing itself, which will be used to determine
     * the node to send to (self-1) and to receive from (self+1) heartbeat messages.
     * @return
     */

    public int getSelfIndex() {
        if (this.nodeList.size() == 0) {
            return 0;
        } else {
            for (int i = 0; i < this.nodeList.size(); i++) {
                if (this.nodeList.get(i).getIP().compareTo(this.selfIP) == 0) {
                    return i;
                }
            }
        }
        return 0;
    }

    /**
     * To get the index of the elected leader node
     * @return
     */
    public int getLeaderIndex() {
        if (this.nodeList.size() == 0) {
            return 0;
        } else {
            for (int i = 0; i < this.nodeList.size(); i++) {
                if (this.nodeList.get(i).getIP().compareTo(this.leaderIP) == 0) {
                    return i;
                }
            }
        }
        return 0;
    }


    public Machines getSelfNode() {
        if (this.nodeList.size() == 0) {
            return null;
        } else {
            return this.nodeList.get(this.selfIndex);
        }
    }

    public void addNodeToList(Machines machine) {
        if (machine.isValid() &&
                !this.nodeList.contains(machine)) {
            this.nodeList.add(machine);
            Collections.sort(this.nodeList);
            this.selfIndex = getSelfIndex();
            this.leaderIndex = getLeaderIndex();
        }
    }

    public void removeNodeFromGroup(Machines machine) {
        if (this.nodeList.contains(machine)) {
            Machines leaderMachines = getLeader();
            if (leaderMachines != null) {
                if (machine.getIP().equals(leaderMachines.getIP())) {
                    if (this.nodeList.size() == 2) {
                        // we are the new leader
                        setSelfAsLeader();
                    } else {
                        this.electionInProgress = true;
                    }
                }
            }
            this.nodeList.remove(machine);
            this.selfIndex = getSelfIndex();
            this.leaderIndex = getLeaderIndex();
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < this.nodeList.size(); i++) {
            sb.append(this.nodeList.get(i).toString());
            if (i != (this.nodeList.size() - 1)) {
                sb.append(",");
            }
        }
        sb.append("]");
        return sb.toString();
    }
}