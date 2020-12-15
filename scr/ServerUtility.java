import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class ServerUtility {

	private static ArrayList<ReplicaNode> allServerList = new ArrayList<ReplicaNode>();
	
	public static ArrayList<ReplicaNode> generateServerReplicationList(String filename, int serverPort){
		
		ArrayList<ReplicaNode> replicaServerList = new ArrayList<ReplicaNode>();
		
		int index = 0;
		
		allServerList = generateServerList(filename);
		
		int tempIndex = 0;
		for(ReplicaNode node : allServerList){
			if(node.port == serverPort){
				index = tempIndex;
			}
			tempIndex++;
		}
		
		//To have replication factor as 3
		replicaServerList.add(allServerList.get(index));
		if(index+1 < allServerList.size()){
			replicaServerList.add(allServerList.get(index+1));
		}else{
			replicaServerList.add(allServerList.get(Math.abs(allServerList.size()-index-1)));
		}
		if(index+2 < allServerList.size()){
			replicaServerList.add(allServerList.get(index+2));
		}else{
			replicaServerList.add(allServerList.get(Math.abs(allServerList.size()-index-2)));
		} 

		return replicaServerList;
	}
	
	public static ArrayList<ReplicaNode> generateServerList(String filename){
		ArrayList<ReplicaNode> allServerList = new ArrayList<ReplicaNode>();
		
		try {
			File fr = new File(filename);
			BufferedReader br = new BufferedReader(new FileReader(fr));
			
			String st;
			while ((st = br.readLine()) != null) {
				String[] serverReplica = st.split(" ");
				ReplicaNode serverInfo = new ReplicaNode();
				serverInfo.id = serverReplica[0];
				serverInfo.ip = serverReplica[1];
				serverInfo.port = Integer.parseInt(serverReplica[2]);
				allServerList.add(serverInfo);
				
			}
			br.close();
			
		} catch (FileNotFoundException e) {
			System.err.println("Error: Input file is Invalid");
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
		} 

		return allServerList;
	}
	
	public static String getServerName(int port){
		
		for(ReplicaNode node : allServerList){
			if(node.port == port){
				return node.id;
			}
		}
		return "temp";
	}
}
