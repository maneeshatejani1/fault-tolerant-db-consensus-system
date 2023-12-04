

import java.io.IOException;
import java.net.InetSocketAddress;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.MyDBReplicatedServer;
import server.ReplicatedServer;
import server.faulttolerance.MyDBFaultTolerantServerZK;

public class ServerRunner {
    public static void main(String[] args) throws IOException {
        NodeConfig<String> nodeConfigServer =  NodeConfigUtils.getNodeConfigFromFile
        ("conf/servers.properties", ReplicatedServer.SERVER_PREFIX, ReplicatedServer
                .SERVER_PORT_OFFSET);
        //System.out.println(nodeConfigServer.getNodeIDs());
       new MyDBFaultTolerantServerZK(nodeConfigServer, "server0", new InetSocketAddress("localhost", 9042));
        
    }
}
