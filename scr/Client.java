import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class Client {

	public static ReplicatedKeyValueStore.Client client;
	
	public static void main(String[] args) throws IOException, SystemException, TException {
		
		ReplicaNode coordinator;
		
		
		if(args.length != 1){
			System.out.println("Input argument is not correct");
			System.exit(0);
		}
		
		String replicaFileName = args[0];
		
		List<ReplicaNode> replList = ServerUtility.generateServerList(replicaFileName);
		
		Random rand = new Random();
		coordinator = replList.get(rand.nextInt(3) + 0);
		
		TTransport transport = new TSocket(coordinator.getIp(), coordinator.getPort());
		System.out.println("Connected to coordinator " + coordinator.getIp() + " " + coordinator.getPort());
		while(true) {
			transport.open();
			System.out.println("Client is ready to accept requests.");
			
			TProtocol protocol = new TBinaryProtocol(transport);
			client = new ReplicatedKeyValueStore.Client(protocol);
			
			System.out.println("Enter name of operation : get or put");
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String operation = br.readLine();
			
			if(operation.equalsIgnoreCase("put")){
				putFlow();
			}else if(operation.equalsIgnoreCase("get")){
				getFlow();
			}else{
				System.out.println("Operatin you have entered is not supported");
			}
			transport.close();
			
		}
	}
	
	public static void putFlow(){
		System.out.println("enter key, value and consistency level (ONE or QUORUM)");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String parameters;
		try {
			parameters = br.readLine();
			String[] input = parameters.split(",");
			if(input.length != 3){
				System.out.println("Input argument is not correct for put request");
				return;
			}
			int key = Integer.parseInt(input[0]);
			String value = input[1];
			String consistancy = input[2];
			if(!(consistancy.equalsIgnoreCase("ONE") || consistancy.equalsIgnoreCase("QUORUM"))){
				System.out.println("Given value for Consistancy is not supported");
				return;
			}
			Request request = new Request();
			request.setLevel(consistancy);
			request.setIsCoordinator(true);
			
			boolean successWriteOperation;
			
			successWriteOperation = client.put(key, value, request, null);
			if(successWriteOperation){
				System.out.println("Write operation is successful");
			}else{
				System.out.println("Write operation failed");
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
	
	public static void getFlow() {
		System.out.println("enter key and consistency level (ONE or QUORUM)");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		try {
			String parameters = br.readLine();
			String[] input = parameters.split(",");
			if(input.length != 2){
				System.out.println("Input argument is not correct for put request");
				return;
			}
			int key = Integer.parseInt(input[0]);
			String consistancy = input[1];
			
			if(!(consistancy.equalsIgnoreCase("ONE") || consistancy.equalsIgnoreCase("QUORUM"))){
				System.out.println("Given value for Consistancy is not supported");
				return;
			}
			
			Request request = new Request();
			request.setIsCoordinator(true);
			request.setLevel(consistancy);
			
			String returnedValue = client.get(key, request, null);
			System.out.println("returnedValue on client "+returnedValue );
			if(returnedValue == null || returnedValue.equals("NULL")) {
				System.out.println("The key either does not exist or has null value");
			} else {
				String[] value1 = returnedValue.split(",", 2);
				System.out.println("The value returned for key " + key + " is " + value1[1]);
			}
		} catch (Exception e) {
			System.out.println("Error in get opration");
		}
		
		
	}

}
