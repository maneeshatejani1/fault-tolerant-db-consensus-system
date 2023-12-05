package server.faulttolerance;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.MyDBReplicatedServer;
import server.ReplicatedServer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.stream.Collectors;

import javax.sound.midi.MidiDevice.Info;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.json.JSONObject;

import com.datastax.driver.core.*;

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
	protected String leaderZnode;
    protected int aliveCount;
    private int deadCount;
    
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
        this.aliveCount = this.serverMessenger.getNodeConfig().getNodeIDs().size();
        deadCount = 0;
        //Setting up Znodes for leader election and Logging          
        try {
            this.zk = new ZooKeeper(ZK_HOST + ":" + DEFAULT_PORT, 3000, this);

			// Check if election path exists
            Thread.sleep(1000);
			Stat statElection = this.zk.exists(ZK_ELECTION_PATH, false);
            if (statElection == null){
                this.zk.create(ZK_ELECTION_PATH, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);	
            }
            else {
                // System.out.println("Election ZNODE already exists");
            }
                
            
			// Create an ephemeral znode for the replica
			this.zk.create(ZK_ELECTION_PATH + "/" + "znode_", this.myID.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

			//Check if Service path already exists
            Thread.sleep(1000);
            Stat statService = this.zk.exists(ZK_SERVICE_PATH , false);
            if (statService == null){
                this.zk.create(ZK_SERVICE_PATH, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            else {
                // System.out.println("Service ZNODE already exists");
            }
                
			
			//Check if server znode already exists and create it if it doesn't exist
            Thread.sleep(1000);
			Stat statServiceServer = zk.exists(ZK_SERVICE_PATH + "/" + this.myID , false);
			if (statServiceServer == null) {
                zk.create(ZK_SERVICE_PATH + "/" + this.myID, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            else {
                // System.out.println("Service path for this server already exists. Implies crashed server has come back up");
            }

            // Set a watch on the replicas znode to monitor changes
            Thread.sleep(1000);
            this.zk.exists(ZK_ELECTION_PATH, true);

            checkLeader(); 
        } catch (KeeperException | InterruptedException | IOException e) {
            e.printStackTrace();
        }

		// TODO: Make sure to do any needed crash recovery here.
        crashRecovery();
	}

    public void crashRecovery(){
        // restore from checkpoint (on cassandra?)
        restoreDataFromCSV();

        // run commands again from logs
        try{
            Thread.sleep(2000);
            byte[] currentData = zk.getData(ZK_SERVICE_PATH + "/" + myID, false, null);
            String[] currentLog = new String(currentData).split("\\n");

            for (String line : currentLog) {
                String[] parts = line.split("\\s+", 2);

                if (parts.length == 2) {
                    String reqId = parts[0];
                    String requestString = parts[1];

                    // Call session.execute with the requestString
                    session.execute(requestString);
                    // System.out.println("Recovered query is "+ requestString);
                }
            }
        } catch (KeeperException | InterruptedException e){
            e.printStackTrace();
        }
    }

    /**
	 * TODO 5: Implement the logic for this.
	 */ 
	public List<String> getAliveNodes() {
        try{
            //get a list of all alive nodes 
            Thread.sleep(2000);
            List<String> children = zk.getChildren(ZK_ELECTION_PATH, false);
            List<String> dataFromChildren = new ArrayList<>();
            for(String child : children){
                String childPath = ZK_ELECTION_PATH + "/" + child;
                byte[] data = zk.getData(childPath, false, null);
                String dataAsString = new String(data);
                dataFromChildren.add(dataAsString);
            }
            return dataFromChildren;
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
	}

    @Override
    public void process(WatchedEvent event) {
        // init msg
        switch (event.getType()) {
            case None:
                // SyncConnected
                break;
        
            case NodeChildrenChanged:
                checkLeader();
                break;

            default:
                break;
        }
    }

    private void checkLeader(){
        try {
            List<String> children = zk.getChildren(ZK_ELECTION_PATH, true);

            // check if leader is gone
            if (!children.contains(this.leaderZnode)){
                electLeader(children);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    private void recoverLeaderQueue() {
        try{
            String QueueString = new String(this.zk.getData(ZK_SERVICE_PATH, false, null), StandardCharsets.UTF_8);
            System.out.println("Leader Logs\n" + QueueString);
            if (QueueString.isEmpty()){
                return;
            }
            String[] Queue = QueueString.split("\n");
            for (String request: Queue){
                //System.out.println("Request in the queue" + request);
                String[] requestParts = request.split(" ", 2); // split({char to split by}, {limit on how many parts its split into})
                for (String part: requestParts){
                    // System.out.println("Part" + part);
                }
                Long reqId = Long.parseLong(requestParts[0]);
                String query = requestParts[1];
                log.log(Level.INFO, "Query is {0}", query);
                // System.out.println("Query is" + query);
                JSONObject json = null;
                try {
                    json = new JSONObject(query);
                    json.put(MyDBClient.Keys.REQNUM.toString(), reqId);
                    queue.put(reqId, json);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
            executeQueue(expected);
        }
        catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    // 'children' meant to be the list of znode children of the election
    private void electLeader(List<String> children) throws KeeperException, InterruptedException{
        try {
            leaderZnode = Collections.min(children);
            String leaderPath = ZK_ELECTION_PATH + "/" + leaderZnode;
            // System.out.println("Leader path is " + leaderPath);
            this.leader = new String(this.zk.getData(leaderPath, false, null), StandardCharsets.UTF_8);
            System.out.println("Leader is " + this.leader);
            if (this.myID == this.leader){
                recoverLeaderQueue();
            }
        }
        catch(KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void exportDataToCSV() {
        ResultSet rsTables = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", session.getLoggedKeyspace());
        List<String> tables = rsTables.all().stream().map(row -> row.getString("table_name")).collect(Collectors.toList());
        for (String tableName : tables) {

            // Use SELECT query to fetch data
            String selectQuery = String.format("SELECT * FROM %s", tableName);
            ResultSet resultSet = session.execute(selectQuery);

            // Write data to CSV file
            try (FileWriter csvWriter = new FileWriter("src/server/faulttolerance/backups/backup.csv", false)) {
                // Get table metadata to determine column names
                TableMetadata tableMetadata = cluster.getMetadata().getKeyspace(myID).getTable(tableName);
                
                for (Row row : resultSet) {
                    // Write data to CSV file dynamically based on table metadata
                    boolean firstColumn = true;

                    for (ColumnMetadata column : tableMetadata.getColumns()) {
                        if (!firstColumn) {
                            csvWriter.append(", ");
                        }

                        csvWriter.append(column.getName());
                        csvWriter.append(":");

                        // Get values from the row dynamically based on column name
                        Object rowValue = row.getObject(column.getName());
                        String csvRowValue = rowValue instanceof ArrayList ? rowValue.toString().replace(" ", "") : rowValue.toString();
                        csvWriter.append(csvRowValue);

                        firstColumn = false;
                    }

                    csvWriter.append("\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void restoreDataFromCSV() {
        ResultSet rsTables = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", session.getLoggedKeyspace());
        List<String> tables = rsTables.all().stream().map(row -> row.getString("table_name")).collect(Collectors.toList());
        for (String tableName : tables) {
            session.execute("TRUNCATE " + tableName);
            // Read data from CSV file and insert into the Cassandra table
            try (BufferedReader csvReader = new BufferedReader(new FileReader("src/server/faulttolerance/backups/backup.csv"))) {
                String line;
                boolean firstLine = true;
                PreparedStatement preparedStatement = null;
                while ((line = csvReader.readLine()) != null) {
                    // Assuming a simple scenario where each CSV line corresponds to a row
                    Map<String, Object> columns = parseCsvLine(line);

                    if(firstLine){
                        // Insert data into Cassandra table dynamically based on column names
                        // Only need to make prepared statement once
                        String insertQuery = String.format("INSERT INTO %s (%s) VALUES (%s)",
                                tableName,
                                String.join(", ", columns.keySet()),
                                String.join(", ", Collections.nCopies(columns.size(), "?")));
                        preparedStatement = session.prepare(insertQuery);
                        firstLine = false;
                    }

                    List<Object> values = new ArrayList<>(columns.values());
                    session.execute(preparedStatement.bind(values.toArray()));
                }                
            } catch (FileNotFoundException e) {
                // Handle the case where the file does not exist
                System.err.println("backup.csv not found: no checkpoints have been made yet");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private Map<String, Object> parseCsvLine(String line) {
        Map<String, Object> columns = new LinkedHashMap<>();

        String[] parts = line.split(", ");
        for (String part : parts) {
            String[] keyValue = part.trim().split(":");
            String key = keyValue[0].trim();
            String value = keyValue[1].trim();
            columns.put(key, convertValue(value));
        }

        return columns;
    }

    private Object convertValue(String value) {
        if (value.contains("]")) {
            // Parse as an array of integers
            String[] parts = value.replaceAll("[\\[\\]]", "").split(",");
            return Arrays.stream(parts)
                        .map(String::trim)
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());

        } else {
            // Parse as a single integer
            try {
                return Integer.parseInt(value.trim());
            } catch (NumberFormatException e) {
                // Handle the case where the value is not a valid integer
                throw new IllegalArgumentException("Invalid integer value: " + value);
            }
        }
    }


    
	/**
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


    protected static enum Type {
		REQUEST, // a server forwards a REQUEST to the leader
		PROPOSAL, // the leader broadcast the REQUEST to all the nodes
        ACKNOWLEDGEMENT; // all the nodes send back acknowledgement to the leader
    }

    /**
     * Logs the request to leader znode at /service
     * Steps:
     * Fetch the contents from the respective znode /service
     * Append the current request message to the fetched value
     * Send new content back to that znode
     */
    private void logRequest(String znode, JSONObject json, Long reqId){
        try{
            String oldLog = new String(this.zk.getData(znode, false, null), StandardCharsets.UTF_8);
            //System.out.println("Old Log: " + oldLog);
            String newLog = oldLog + "\n" + reqId + " " + json.toString();
            //System.out.println("New Log: " + newLog);
            this.zk.setData(znode,newLog.getBytes(), -1);
            String savedLog = new String(this.zk.getData(znode, false, null), StandardCharsets.UTF_8);
            log.log(Level.INFO, "Saved Log at the znode {0}: ", savedLog);
            //System.out.println(savedLog + "Saved Log at the znode");
        }
        catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void logProposal(String query, Long reqId){
        try{
            // Get current log
            byte[] currentData = zk.getData(ZK_SERVICE_PATH + "/" + myID, false, null);
            String currentLog = new String(currentData);
            String newLog;

            // Check if the log has reached MAX_LOG_SIZE, make checkpoint and clear log if so
            if (currentLog.split("\\n").length >= MAX_LOG_SIZE) {
                exportDataToCSV();
                newLog = reqId + " " + query;
            } else {
                // else just append to the log
                newLog = currentLog + "\n" + reqId + " " + query;
            }
            
            // set new log
            zk.setData(ZK_SERVICE_PATH + "/" + myID, newLog.getBytes(), -1);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    private void removeRequestFromLog(String znode){
        try{
            String oldLog = new String(this.zk.getData(znode, false, null), StandardCharsets.UTF_8);
            String newLog = oldLog.substring(oldLog.indexOf("\n")+1);
            if (!newLog.contains("/n")){
                newLog = "";
            }
            this.zk.setData(znode,newLog.getBytes(), -1);

        }
        catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

	/**
	 */
	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {        

        // deserialize the request
        JSONObject json = null;
		try {
			json = new JSONObject(new String(bytes));
		} catch (JSONException e) {
			e.printStackTrace();
		}
        
        log.log(Level.INFO, "{0} received relayed message {1} from {2}",
                new Object[]{this.myID, json, header.sndr}); // simply log
        
        // check the type of the request
        try {
			String type = json.getString(MyDBClient.Keys.TYPE.toString());
			if (type.equals(Type.REQUEST.toString())){
				if(myID.equals(leader)){
					
					// put the request into the queue
					Long reqId = incrReqNum();
					json.put(MyDBClient.Keys.REQNUM.toString(), reqId);

                    //log the request to leader znode in case server crashes here
                    logRequest(ZK_SERVICE_PATH, json,reqId);
					queue.put(reqId, json);
					log.log(Level.INFO, "{0} put request {1} into the queue.",
			                new Object[]{this.myID, json});
					
                    executeQueue(expected);
				} else {
					log.log(Level.SEVERE, "{0} received REQUEST message from {1} which should not be here.",
			                new Object[]{this.myID, header.sndr});
				}
			} else if (type.equals(Type.PROPOSAL.toString())) {
				
				// execute the query and send back the acknowledgement
                // System.out.println("Received a PROPOSAL");
				String query = json.getString(MyDBClient.Keys.REQUEST.toString());
				long reqId = json.getLong(MyDBClient.Keys.REQNUM.toString());

                //Log Request to it's znode
                logProposal(query, reqId);
				

				session.execute(query);
                // System.out.println("Query is " + query);
				
				JSONObject response = new JSONObject().put(MyDBClient.Keys.RESPONSE.toString(), this.myID)
						.put(MyDBClient.Keys.REQNUM.toString(), reqId)
						.put(MyDBClient.Keys.TYPE.toString(), Type.ACKNOWLEDGEMENT.toString());
				serverMessenger.send(header.sndr, response.toString().getBytes());
			} else if (type.equals(Type.ACKNOWLEDGEMENT.toString())) {
				
				// only the leader needs to handle acknowledgement
				if(myID.equals(leader)){
					String node = json.getString(MyDBClient.Keys.RESPONSE.toString());
					if (dequeue(node)){
						// if the leader has received all acks, then prepare to send the next request
                        System.out.println("Received acknowledgments from all alive Servers");
						expected++;
						executeQueue(expected);
					}
				} else {
					log.log(Level.SEVERE, "{0} received ACKNOWLEDEMENT message from {1} which should not be here.",
			                new Object[]{this.myID, header.sndr});
				}
			} else {
				log.log(Level.SEVERE, "{0} received unrecongonized message from {1} which should not be here.",
		                new Object[]{this.myID, header.sndr});
			}
			
		} catch (JSONException | IOException e1) {
			e1.printStackTrace();
		}
    }

    
    /**
     * Execute message at head of the queue when required(i.e when the previous proposal got ACked by all)
	 */
    private void executeQueue(long expected) {
        if(isReadyToSend(expected)){
                // retrieve the first request in the queue
                try {
                    JSONObject proposal = queue.remove(expected);
                    if(proposal != null) {
                        proposal.put(MyDBClient.Keys.TYPE.toString(), Type.PROPOSAL.toString());
                        enqueue();
                        broadcastRequest(proposal);
                        removeRequestFromLog(ZK_SERVICE_PATH);
                    } else {
                        log.log(Level.INFO, "{0} is ready to send request {1}, but the message has already been retrieved.",
                                new Object[]{this.myID, expected});
                    }
                } catch (JSONException e){
                    e.printStackTrace();
                }
						
        }
    }

    private boolean isReadyToSend(long expectedId) {
		if (queue.size() > 0 && queue.containsKey(expectedId)) {
			return true;
		}
		return false;
	}
	
    /**
	* TODO 6: Change the logic of this function to braodcast requests to only alive nodes and then for dead nodes, log the request in their respective znode so that they know where to pick up from
	*/
	private void broadcastRequest(JSONObject req) {
        try {
            Set<String> allNodes = this.serverMessenger.getNodeConfig().getNodeIDs();
            List<String> aliveNodes = getAliveNodes();
            Long reqId = req.getLong(MyDBClient.Keys.REQNUM.toString());
            String query = req.getString(MyDBClient.Keys.REQNUM.toString());
            for (String node : allNodes){
                if (!aliveNodes.contains(node)) {
                    Stat statServiceServer = zk.exists(ZK_SERVICE_PATH + "/" + node , false);
                    if (statServiceServer == null) {
                        zk.create(ZK_SERVICE_PATH + "/" + node, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                    // Get current log
                    byte[] currentData = zk.getData(ZK_SERVICE_PATH + "/" + node, false, null);
                    String currentLog = new String(currentData);
                    String newLog;

                    // simply append to log
                    // TODO handle case where log overflows MAX_SIZE_LOG
                    newLog = currentLog + "\n" + reqId + " " + query;
                    
                    // set new log
                    zk.setData(ZK_SERVICE_PATH + "/" + node, newLog.getBytes(), -1);
                } else {
                    this.serverMessenger.send(node, req.toString().getBytes());
                    System.out.println("Sent Proposal to server" + node);
                }
            }
            log.log(Level.INFO, "The leader has broadcast the request {0}", new Object[]{req});
        } catch (KeeperException | InterruptedException | IOException | JSONException e ) {
            e.printStackTrace();
        }
	}
	
	private void enqueue(){
		notAcked = new CopyOnWriteArrayList<String>();
        Set<String> aliveChildren = new HashSet<>(getAliveNodes());
        System.out.println("Alive Nodes" + aliveChildren.toString());
		for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()){
            if (aliveChildren.contains(node)) {
                notAcked.add(node);
            }
		}
        System.out.println("Nodes that have to acknowlede" + notAcked.toString());
	}
	
    /*
    * TODO 7: Change the logic of the Acknowledgement system such that either
    * a) only a majority of ACKs are needed 
    * b) only alive node ACKs are needed
    */
	private boolean dequeue(String node) {
		if(!notAcked.remove(node)){
			log.log(Level.SEVERE, "The leader does not have the key {0} in its notAcked", new Object[]{node});
		}
        else{
            System.out.println("Received acknowldegment from node" + node);
        }
		if(notAcked.size() == 0)
			return true;
		return false;
	}
	

	/**
	 * TODO: Clean up tables and all 
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