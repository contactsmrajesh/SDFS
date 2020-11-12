# cpen533-2020-pa3-subramani

### **OVERVIEW**
>
>The project implements FS533, a distributed file system whihc performs put, get, delete and list operations.This functionality leverages the group membership services and logging mechanism along with grep functionality implemented earlier.
>
>The **put operation**, will place the file in the initiator node fs533 path and then will replicate it to 2 other nodes in the group membership service. So, totally the file will be present in **3 different nodes**, this design can withstand a failure of 2 nodes. The **get operation** will look for the file locally first, this is to retrieve it fast, if not present locally then it will connect to the leader node to get the IP of one of the replicated nodes and then retrieve the file.  The **delete operation** will remove the file from all the 3 replicated nodes. The **list operations** can be performed to list files in FS533, local files and locate files in FS533. The **leader election algorithm** elects the leader with largest IP from the group membership service.
> 

### **DESIGN IMPLEMENTATION**

> Below is a brief about the files included forming the key components of the design:
> 1. **App.java** : The App service is the interface between the user and the distributed system service which also leverages the group membership services and the grep functionality. 
>
> 2. **Fs533Client.java, Fs533Server.java, Fs533GetFileHelper.java & Fs533PutFileHelper.java** : Implements the operations of put, get, delete and list as a part of the fs533 distributed filesystem. 
>
> 3. **HeartBeatFDClient.java & HeartBeatFDServer.java** : Implements Leader election using the Bully algorithm which uses a global timeout variable to determine if a specific node has to announce and declare itself the new leader and failure detection using a bounded buffer through the producer and consumer threads. 
>
> The Application will prompt the user to use any of the below options,
> 1. **put \<local_file_name\> \<fs533_file_name\>** : To add a local file to fs533 with the given fs533 name.
> 2. **get \<fs533_file_name\> \<local_file_name\>** : To fetch a fs533 file to the local machine.
> 3. **remove \<fs533filename\>** : To remove a file from fs533
> 4. **ls** : To list all files in fs533
> 5. **locate \<fs533_file_name\>** : To locate all the nodes that contain a copy of the file.
> 6. **lshere** : To list all the fs533 files stored on the local machine.
> 7. **pl** : To print the leader node
> 8. **s** : To query the Application logs
> 9. **j** : For a member to join the group membership service
> 10. **l** : For a member to leave the group membership service
> 11. **p** : To print the group membership list
> 12. **e** : To exit the Application


### **INSTRUCTIONS TO RUN THE APPLICATION**

#### **STEP 1 : TRIGGER EC2 Instances**
> 1. EC2 instances (virtual machines) are created so that we can have our own small distributed system with below configurations in each instance:
> Code to configure:
> - Java Installation:
>   - wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u141-b15/336fa29ff2bb4ef291e347e091f7f4a7/jdk-8u141-linux-x64.rpm
>   - sudo yum install -y jdk-8u141-linux-x64.rpm
> - Creation of required directories 
>   - mkdir jars
> - Pull the latest built jars,
>   - cd ../jars
>   - aws s3 sync s3://s3-bucket/jars/ .

#### **STEP 2 : RUN THE FS533 APPLICATION**
> 1.  ***Run Command :*** 
> java -cp pa2-1.0-SNAPSHOT-jar-with-dependencies.jar com.ers.pa1.App
> 2. Give user inputs based on the prompt values as listed above.
>

