

import java.io.IOException;
import java.net.InetSocketAddress;

import client.Client;

public class ClientRunner {
    public static void main(String[] args) throws IOException, InterruptedException {
        Client client = new Client();
        for(int i=1;i<10;i++){
            client.send(new InetSocketAddress("localhost", 2000+(i%3)), "**** Message #"+ i +" ******");
        }
        // client.send(new InetSocketAddress("localhost", 2000), "**** Message #"+(1)+" ******");
        // client.send(new InetSocketAddress("localhost", 2001), "**** Message #"+(2)+" ******");
        // client.send(new InetSocketAddress("localhost", 2002), "**** Message #"+(23)+" ******");
        
        
    }
}
