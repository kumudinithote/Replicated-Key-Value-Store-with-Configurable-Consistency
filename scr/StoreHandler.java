import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class StoreHandler implements ReplicatedKeyValueStore.Iface{

	private String id;
	private int port;
	private String ip;
	private List<ReplicaNode> replicaList;
	private List<ReplicaNode> allServerList;
	private Map<Integer, Data> store;
	public static final int keyCapacity = 64;
	public static int minKey;
	public static int maxKey;
	private Map<String, List<FailedOperation>> hints;

	public StoreHandler(String id, String ip, int port, List<ReplicaNode> replList, List<ReplicaNode> allServerList) throws NumberFormatException, IOException {
		this.id = id;
		this.port = port;
		this.ip = ip;
		this.replicaList = replList;
		this.allServerList = allServerList;
		store = new ConcurrentHashMap<Integer, Data>();
		
		File logFile = new File(id+".log");
		
		if (logFile.exists()){
			System.out.println("Server was offline, restarting..");
			bootStrap(logFile);
		}else{
			System.out.println("Server is gettig initilized for the first time.");
			logFile.createNewFile();
		}
		
		int tempKey = Integer.parseInt(id) - 1;
		minKey = keyCapacity * tempKey;
		maxKey = minKey + 63;
		
		hints = new ConcurrentHashMap<String, List<FailedOperation>>();
		
		System.out.println("Server : "+id+" is responsible for holding keys from "+minKey+" to "+maxKey);
		
	}
	
	@Override
	public boolean put(int key, String value, Request request, ReplicaNode replicaID) {
		
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		Data oldValue = store.get(key);
		boolean isSuccessfull = false;
		//current person is owner
		if((minKey <= key && key <= maxKey) || replicaID != null){
			try {
				updateLog(key, new Data(value, timestamp));
				
				if (request.isIsCoordinator()) {
					isSuccessfull = putOnReplicas(key, value,
							request.setIsCoordinator(false).setTimestamp(timestamp.toString()));
					if (!isSuccessfull && oldValue != null) {
							updateLog(key, oldValue);
							store.put(key, oldValue);
						
					}
					return isSuccessfull;
				}
				
			} catch (IOException e) {
				System.out.println(e);
			}
			
			
			if (oldValue == null) {
				System.out.println("No key found, writng with current timestamp");
				Data valueWithTimestamp = new Data(value, timestamp);
				store.put(key, valueWithTimestamp);
			}
			
			if (oldValue != null && oldValue.getTimestamp().before(timestamp)) {
				
				System.out.println("Key already exist, updating key with value and current timestamp");
				Data valueWithTimestamp = new Data(value, timestamp);
				store.put(key, valueWithTimestamp);
			}
		}
		else{
			System.out.println("Finding the real owner.");
			int tempNodeKey = key/keyCapacity;
			
			ReplicaNode keyOwner = allServerList.get(tempNodeKey);
			TTransport tTransport = new TSocket(keyOwner.getIp(), keyOwner.getPort());
			try {
				tTransport.open();
				TProtocol tProtocol = new TBinaryProtocol(tTransport);
				ReplicatedKeyValueStore.Client client = new ReplicatedKeyValueStore.Client(tProtocol);
				client.put(key, value, request, null);
			} catch (TTransportException e) {
				e.printStackTrace();
			}catch (SystemException e) {
				e.printStackTrace();
			} catch (TException e) {
				e.printStackTrace();
			}
			
			tTransport.close();
		}
		
		//performing HintedHandoff operation
		performHintedHandoff();
		
		return true;
	}
	
	@Override
	public String get(int key, Request request, ReplicaNode replicaID) throws SystemException, TException {
		
		String returnValue = new String("NULL");
		
		if((minKey <= key && key <= maxKey) || replicaID != null){
			System.out.println("Original owner");
			if (request.isIsCoordinator()) {
				returnValue = readFromReplicas(key, request);
				
			} else {
				if (store.get(key) != null) {
					returnValue = store.get(key).getTimestamp() + "," + store.get(key).getData();
				}
			}
		}else{
			System.out.println("Finding the real owner.");
			int tempNodeKey = key/keyCapacity;
			
			ReplicaNode keyOwner = allServerList.get(tempNodeKey);
			TTransport tTransport = new TSocket(keyOwner.getIp(), keyOwner.getPort());
			tTransport.open();
			TProtocol tProtocol = new TBinaryProtocol(tTransport);
			ReplicatedKeyValueStore.Client client = new ReplicatedKeyValueStore.Client(tProtocol);
			
			returnValue = client.get(key, request, null);
						
			tTransport.close();
		}
		
		//performing HintedHandoff operation
		performHintedHandoff();
		
		return returnValue;
	}
	
	private void bootStrap(File logFile) throws NumberFormatException, IOException{
		FileReader reader = new FileReader(logFile);
		BufferedReader br = new BufferedReader(reader);

		String line;
		while ((line = br.readLine()) != null) {
			String[] entry = line.split(",");
			System.out.println("Data from the log " + entry[0] + "," + entry[2] + "," + entry[1]);
			store.put(Integer.parseInt(entry[0]), new Data(entry[2], Timestamp.valueOf(entry[1])));
		}
		br.close();
	}
	
	private void updateLog(int key, Data value) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(id+".log", true));
		String logLine = key + "," + value.getTimestamp() + "," + value.getData();
		bw.write(logLine);
		bw.newLine();
		bw.close();
	}
	
	
	private String readFromReplicas(int key, Request request) {
		int consistencyLevel;
		List<Data> valueList = new ArrayList<Data>();
		
		String result = null;

		if (request.getLevel().equalsIgnoreCase("ONE")) {
			consistencyLevel = 1;
		} else {
			consistencyLevel = 2;
		}

		System.out.println("size of replicalist & consistencyLevel: "+replicaList.size()+"  "+consistencyLevel);
		for (ReplicaNode replicaID : replicaList) {
			try {
				
				//get latest value
				if (valueList.size() >= consistencyLevel) {
					Data newestValue = valueList.get(0);
					for (Data value : valueList) {
						if (newestValue.getTimestamp().compareTo(value.getTimestamp()) < 0) {
							newestValue = value;
						}
					}
					
					result = newestValue.getTimestamp() + "," + newestValue.getData();
				}
				
				Data value = readFromNode(replicaID, key, request);
				if(value != null) valueList.add(value);
			} catch (Exception e) {
				System.out.println("Error while reading replica: " + replicaID.getId());
			}

		}
		
		return result;
	}
	
	private Data readFromNode(ReplicaNode replicaID, int key, Request request){
		
		Data value = null;
		try{
			TTransport tTransport = new TSocket(replicaID.getIp(), replicaID.getPort());
			tTransport.open();
			TProtocol tProtocol = new TBinaryProtocol(tTransport);
			ReplicatedKeyValueStore.Client client = new ReplicatedKeyValueStore.Client(tProtocol);

			String val = client.get(key, request.setIsCoordinator(false), replicaID);
			String[] tsVal = val.split(",", 2);
			value = new Data(tsVal[1], java.sql.Timestamp.valueOf(tsVal[0]));
			
			tTransport.close();
			
			
		}catch(Exception e){
			System.out.println("Not able to connect to replica");
		}
		
		return value;
	}
	
	private boolean putOnReplicas(int key, String value, Request request) {
		int successCount = 0;
		boolean writeSuccessful = false;
		
		int count = request.getLevel().equalsIgnoreCase("ONE") ? 1 : 2;
		
		for (ReplicaNode replicaID : replicaList) {
			try {
				TTransport tTransport = new TSocket(replicaID.getIp(), replicaID.getPort());
				tTransport.open();
				TProtocol tProtocol = new TBinaryProtocol(tTransport);
				ReplicatedKeyValueStore.Client client = new ReplicatedKeyValueStore.Client(tProtocol);
				
				ReplicaNode replica = new ReplicaNode();
				replica.id = id;
				replica.ip = ip;
				replica.port = port;
				
				if (client.put(key, value, request, replica)) {
					successCount += 1;
					
					if (successCount >= count) {
						writeSuccessful = true;
					}
				}
				
				tTransport.close();
			}catch (TTransportException e) {
				System.out.println("Could not connect to server " + replicaID.getPort());
				
				System.out.println("Storing failed operation locally");
				
				FailedOperation hint = new FailedOperation(replicaID, key, value, request);
				List<FailedOperation> list = hints.containsKey(replicaID.getId()) ? hints.get(replicaID.getId()) : new ArrayList<FailedOperation>();
				
				list.add(hint);
				hints.put(replicaID.getId(), list);
				
			}catch (Exception e) {
				System.out.println("TException: " +e.getMessage());
			}
		}
		return writeSuccessful;
	}
	
	private void performHintedHandoff() {
		
		for(ReplicaNode replicaID : replicaList){
			List<FailedOperation> listOfHints = hints.get(replicaID.getId());
			
			if(listOfHints != null && listOfHints.size() != 0){
				System.out.println("Start performing Hinted Handoff"); 
				for (FailedOperation hint : listOfHints) {
					TTransport tTransport = new TSocket(replicaID.getIp(), replicaID.getPort());
					try {
						tTransport.open();
						TProtocol tProtocol = new TBinaryProtocol(tTransport);
						ReplicatedKeyValueStore.Client client = new ReplicatedKeyValueStore.Client(tProtocol);
						
						ReplicaNode replica = new ReplicaNode();
						replica.id = id;
						replica.ip = ip;
						replica.port = port;
						client.put(hint.getKey(), hint.getValue(), hint.getRequest(), replica);
						tTransport.close();
					} catch (TTransportException e) {
						e.printStackTrace();
					}catch (TException e) {
						e.printStackTrace();
					}
					
				}
				hints.remove(replicaID.getId());
			}
			
		}

	}
	
	private class Data {
		
		String key;
		Timestamp timestamp;
		
		public Data(String key, Timestamp timestamp) {
			this.key = key;
			this.timestamp = timestamp;
		}

		public String getData() {
			return key;
		}

		public Timestamp getTimestamp() {
			return timestamp;
		}

	}
	
	private class FailedOperation {
		
		private ReplicaNode replicaID;
		private int key;
		private String value;
		private Request request;

		public FailedOperation(ReplicaNode replicaID, int key, String value, Request request) {
			this.replicaID = replicaID;
			this.key = key;
			this.value = value;
			this.request = request;
		}

		public ReplicaNode getReplicaID() {
			return replicaID;
		}

		public int getKey() {
			return key;
		}

		public String getValue() {
			return value;
		}

		public Request getRequest() {
			return request;
		}
	}

}
