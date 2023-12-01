package server.faulttolerance;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.MyDBReplicatedServer;
import server.ReplicatedServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import org.apache.zookeeper.*;
import org.json.JSONException;
import org.json.JSONObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import client.MyDBClient;




/**
 * This class should implement your replicated fault-tolerant database server if
 * you wish to use Zookeeper or other custom consensus protocols to order client
 * requests.
 * <p>
 * Refer to {@link server.ReplicatedServer} for a starting point for how to do
 * server-server messaging or to {@link server.MyDBReplicatedServer} for a
 * non-fault-tolerant replicated server.
 * <p>
 * You can assume that a single *fault-tolerant* Zookeeper server at the default
 * host:port of localhost:2181 and you can use this service as you please in the
 * implementation of this class.
 * <p>
 * Make sure that both a single instance of Cassandra and a single Zookeeper
 * server are running on their default ports before testing.
 * <p>
 * You can not store in-memory information about request logs for more than
 * {@link #MAX_LOG_SIZE} requests.
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer implements Watcher{

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 */
	public static final int SLEEP = 1000;

	/**
	 * Set this to true if you want all tables drpped at the end of each run
	 * of tests by GraderFaultTolerance.
	 */
	public static final boolean DROP_TABLES_AFTER_TESTS=true;

	/**
	 * Maximum permitted size of any collection that is used to maintain
	 * request-specific state, i.e., you can not maintain state for more than
	 * MAX_LOG_SIZE requests (in memory or on disk). This constraint exists to
	 * ensure that your logs don't grow unbounded, which forces
	 * checkpointing to
	 * be implemented.
	 */
	public static final int MAX_LOG_SIZE = 400;

	public static final int DEFAULT_PORT = 2181;
    private static final String ZK_HOST = "localhost";
    private static final String ZK_ELECTION_PATH = "/election";
    private static final String ZK_SERVICE_PATH = "/service";
    private ZooKeeper zk;

    final private Session session;
    final private Cluster cluster;

    protected final String myID;
    protected final MessageNIOTransport<String,String> serverMessenger;

    protected String leader;
    
    // this is the message queue used to track which messages have not been sent yet
    private ConcurrentHashMap<Long, JSONObject> queue = new ConcurrentHashMap<Long, JSONObject>();
    private CopyOnWriteArrayList<String> notAcked;
    
    // the sequencer to track the most recent request in the queue
    private static long reqnum = 0;
    synchronized static Long incrReqNum() {
    	return reqnum++;
    }
    
    // the sequencer to track the next request to be sent
    private static long expected = 0;
    synchronized static Long incrExpected() {
    	return expected++;
    }

    protected static enum Type {
		REQUEST, // a server forwards a REQUEST to the leader
		PROPOSAL, // the leader broadcast the REQUEST to all the nodes
        ACKNOWLEDGEMENT; // all the nodes send back acknowledgement to the leader
	}

	/**
	 * @param nodeConfig Server name/address configuration information read
	 *                      from
	 *                   conf/servers.properties.
	 * @param myID       The name of the keyspace to connect to, also the name
	 *                   of the server itself. You can not connect to any other
	 *                   keyspace if using Zookeeper.
	 * @param isaDB      The socket address of the backend datastore to which
	 *                   you need to establish a session.
	 * @throws IOException
	 */
	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String
			myID, InetSocketAddress isaDB) throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer
						.SERVER_PORT_OFFSET), isaDB, myID);
        session = (cluster=Cluster.builder().addContactPoint("127.0.0.1")
                .build()).connect(myID);
        
        this.myID = myID;

        this.serverMessenger =  new
                MessageNIOTransport<String, String>(myID, nodeConfig,
                new
                        AbstractBytePacketDemultiplexer() {
                            @Override
                            public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                                handleMessageFromServer(bytes, nioHeader);
                                return true;
                            }
                        }, true);
                    
        // Connect to Zookeeper server
        try {
            this.zk = new ZooKeeper(ZK_HOST + ":" + DEFAULT_PORT, 3000, this);

            // Create a znode for the replica
            zk.create(ZK_ELECTION_PATH + "/" + this.myID, this.myID.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zk.create(ZK_SERVICE_PATH + "/" + this.myID, this.myID.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            // Set a watch on the replicas znode to monitor changes
            zk.exists(ZK_ELECTION_PATH, true);

            // elect a leader using cassandra's znodes 
            List<String> children = zk.getChildren(ZK_ELECTION_PATH, true);
            electLeader(children); 
        } catch (KeeperException | InterruptedException | IOException e) {
            e.printStackTrace();
        }

		// TODO: Make sure to do any needed crash recovery here.
        crashRecovery();
	}

    public void crashRecovery(){
        // restore from checkpoint (on cassandra?)
        // run commands again from logs
    }


    @Override
    public void process(WatchedEvent event) {
        // init msg
        switch (event.getType()) {
            case None:
                // SyncConnected
                break;
        
            case NodeChildrenChanged:
                handleChildenChanged();
                break;

            default:
                break;
        }
    }

    private void electLeader(List<String> children) throws KeeperException, InterruptedException{
        this.leader = Collections.max(children);
        if (this.myID == this.leader){
            // add leader znode
        }
        return;

    }

    private void handleChildenChanged(){
        try {
            // check if leader is gone
            List<String> children = zk.getChildren(ZK_ELECTION_PATH, true);

            // if leader gone, elect
            if (!children.contains(this.leader)){
                electLeader(children);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
	/**
	 * TODO: process bytes received from clients here.
	 */
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		// this is a request sent by callbackSend method
		String request = new String(bytes);
		
		log.log(Level.INFO, "{0} received client message {1} from {2}",
                new Object[]{this.myID, request, header.sndr});
        JSONObject json = null;
        try {
            json = new JSONObject(request);
            request = json.getString(MyDBClient.Keys.REQUEST
                    .toString());
        } catch (JSONException e) {
            //e.printStackTrace();
        }
		
		// forward the request to the leader as a proposal        
		try {
			JSONObject packet = new JSONObject();
			packet.put(MyDBClient.Keys.REQUEST.toString(), request);
			packet.put(MyDBClient.Keys.TYPE.toString(), Type.REQUEST.toString());			
			
			this.serverMessenger.send(leader, packet.toString().getBytes());
			log.log(Level.INFO, "{0} sends a REQUEST {1} to {2}", 
					new Object[]{this.myID, packet, leader});
		} catch (IOException | JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        
        
        String response = "[success:"+new String(bytes)+"]";       
        if(json!=null){
        	try{
        		json.put(MyDBClient.Keys.RESPONSE.toString(),
                        response);
                response = json.toString();
            } catch (JSONException e) {
                e.printStackTrace();
        	}
        }
        
        try{
	        // when it's done send back response to client
	        serverMessenger.send(header.sndr, response.getBytes());
        } catch (IOException e) {
        	e.printStackTrace();
        }
	}

	/**
	 * TODO: process bytes received from fellow servers here.
	 */
	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
		throw new RuntimeException("Not implemented");
	}


	/**
	 * TODO: Gracefully close any threads or messengers you created.
	 */
	public void close() {
        super.close();
	    this.serverMessenger.stop();
	    session.close();
	    cluster.close();
	}

	public static enum CheckpointRecovery {
		CHECKPOINT, RESTORE;
	}

	/**
	 * @param args args[0] must be server.properties file and args[1] must be
	 *             myID. The server prefix in the properties file must be
	 *             ReplicatedServer.SERVER_PREFIX. Optional args[2] if
	 *             specified
	 *             will be a socket address for the backend datastore.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile
				(args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer
						.SERVER_PORT_OFFSET), args[1], args.length > 2 ? Util
				.getInetSocketAddressFromString(args[2]) : new
				InetSocketAddress("localhost", 9042));
	}

}