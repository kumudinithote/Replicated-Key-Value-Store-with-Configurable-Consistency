import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

public class Server {

	public static int port;
	public static String ip;
	private static String ServerName = null;
	private static StoreHandler storeHandler;
	private static ReplicatedKeyValueStore.Processor<ReplicatedKeyValueStore.Iface> storeProcessor;
	
	public static void main(String[] args) throws NumberFormatException, IOException {

		String replicaFileName = null;
		
		try {
			port = Integer.parseInt(args[0]);
			ip = InetAddress.getLocalHost().getHostAddress();
			
			replicaFileName = args[1];
			
			System.out.println("Server is started.."+ port +" "+ip);
		} catch (Exception e) {
			System.err.println("Error in starting server" + e.getMessage());
			System.exit(0);
		}
		
		List<ReplicaNode> replList = ServerUtility.generateServerReplicationList(replicaFileName, port);
		List<ReplicaNode> allServerList = ServerUtility.generateServerList(replicaFileName);
		
		ServerName = ServerUtility.getServerName(port);
		
		System.out.println("Server name is :"+ ServerName);
		
		storeHandler = new StoreHandler(ServerName, ip, port, replList, allServerList);
		storeProcessor = new ReplicatedKeyValueStore.Processor<ReplicatedKeyValueStore.Iface>(storeHandler);
		
		Runnable simple = new Runnable() {
			public void run() {
				try {
					startServer(storeProcessor);
				} catch (TTransportException e) {
					System.out.println("Error in starting server");
				}
			}
		};
		new Thread(simple).start();
	}

	public static void startServer(ReplicatedKeyValueStore.Processor<ReplicatedKeyValueStore.Iface> processor) throws TTransportException {
		TServerTransport serverTransport = new TServerSocket(port);

		TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
		System.out.println("************Server Information****************");
		System.out.println("Server NodeID: " + ServerName);
		System.out.println("Server IP: " + ip);
		System.out.println("nServer Port: " + port);
		System.out.println("**********************************************");
		server.serve();
	}
	
	
	
}