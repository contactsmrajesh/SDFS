package com.ers.pa1;

import java.util.Date;

/**
 * Machines class represents a single node in the cluster
 * which is serialized reflecting its respective data that will be communicated
 */

public class Machines implements Comparable<Machines> {
    private long timestamp;
    private String ip="";
    private int    port=0;
    public long lastUpdatedTimestamp = -1;
    private boolean valid = false;


    /**
     *Constructor
     * @param _ip represents the node IP
     * @param _port represents the node port
     * @throws IllegalArgumentException
     */
    public Machines(String _ip, int _port) throws IllegalArgumentException
    {
        this(new Date().getTime(), _ip, _port);
        init();
    }

    /**
     * Constructor
     * @param _timestamp represents the timestamp the node instance was created
     * @param _ip represents the node IP
     * @param _port represents the node port
     */
    public Machines(long _timestamp, String _ip, int _port)
    {
        this.timestamp = _timestamp;
        this.ip = _ip;
        this.port = _port;
        init();
    }

    /**
     * Constructor
     * @param _timestamp represents the timestamp the node instance was created
     * @param _ip represents the node IP
     * @param _port represents the node port
     * @param _lastUpdatedTimestamp represents the timestamp that the node was last updated
     */

    public Machines(long _timestamp, String _ip, int _port, long _lastUpdatedTimestamp)
    {
        this.timestamp = _timestamp;
        this.ip = _ip;
        this.port = _port;
        this.lastUpdatedTimestamp = _lastUpdatedTimestamp;
        init();
    }

    //Called within the constructor to set valid IP value
    private void init()
    {
        valid = isValidIP();
    }

    /**
     * This method is implemented to override the compareTo function so that it sorts the Machine object
     * according to the IP address
     */
    @Override
    public int compareTo(Machines otherMachine)
    {
        String ipParts[] = this.ip.split("[.]");
        String otherIpParts[] = otherMachine.getIP().split("[.]");

        for(int i=0;i<ipParts.length;i++)
        {
            try {
                int currComparison = Integer.valueOf(ipParts[i]).compareTo(Integer.valueOf(otherIpParts[i]));
                if(currComparison != 0)
                {
                    return currComparison;
                }

            } catch(NumberFormatException e) {
            }
        }
        return 0;
    }

    /**
     * Method to compare the last updated timestamp between two machines
     * @param otherMachine represents that target machine to compare the current machine's last updated timestamp with
     * @return
     */
    public int lastUpdatedCompareTo(Machines otherMachine)
    {
        return Long.valueOf(this.lastUpdatedTimestamp).compareTo(Long.valueOf(otherMachine.lastUpdatedTimestamp));
    }

    /**
     * This method overrides the equals method to compare the IP of two node instances
     * @param object
     * @return
     */
    @Override
    public boolean equals(Object object)
    {
        boolean result = false;

        if (object != null && object instanceof Machines)
        {
            result = this.ip.equals(((Machines) object).ip);
        }

        return result;
    }

    //Retrieves the Ip of the node instance
    public String getIP()
    {
        return this.ip;
    }

    //public int getPort() { return this.port; }
    //public long getTimestamp() { return this.timestamp; }

    public boolean isValid()
    {
        return valid;
    }

    //Method to check if a node IP is valid
    private boolean isValidIP()
    {
        String ipParts[] = this.ip.split("[.]");
        if(ipParts.length != 4)
        {
            return false;
        }
        for(int i=0;i<ipParts.length;i++)
        {
            try {
                int currInt = Integer.parseInt(ipParts[i]);
                if(currInt < 0 || currInt > 256)
                {
                    return false;
                }
            } catch(NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString()
    {
        return this.timestamp + ":" + this.ip + ":" + this.port;
    }

    public String verboseToString()
    {
        return "<Node "+this.timestamp + ":" + this.ip + ":" + this.port + ", lastUpdated: " +this.lastUpdatedTimestamp+">";
    }
}