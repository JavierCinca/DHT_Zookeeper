package es.upm.dit.dscc.DHT;

import java.util.HashMap;

import java.util.Set;

public class TableManager {

	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;

	private int       nReplica;
	private int       nServersMax;
	private String   localAddress;
	private HashMap<Integer, String> DHTServers = new HashMap<Integer, String>();
	private HashMap<Integer, DHTUserInterface> DHTTables = new HashMap<Integer, DHTUserInterface>();

	public TableManager(String localAddress,
			int     nServersMax, 
			int     nReplica) {
		this.localAddress = localAddress;
		this.nServersMax  = nServersMax;
		this.nReplica     = nReplica;
	}
	
	public void setLocalAddress(String address) {
		this.localAddress = address;
	}
	
	public String getLocalAddress() {
		return this.localAddress;
	}


	public Integer getPos (String key) {
		int hash =	key.hashCode();
		if (hash < 0) {
			LOGGER.finest("Hash value is negative!!!!!");
			hash = -hash;
		}

		int segment = Integer.MAX_VALUE / (nServersMax); // No negatives

		for(int i = 0; i < nServersMax; i++) {
			if (hash >= (i * segment) && (hash <  (i+1)*segment)){
				return i;
			}
		}
		LOGGER.warning("getPos: This sentence shound not run");
		return 1;
	}

	public Integer getPosition (String address) {
		int posAddress = 0;
		for (int i = 0; i < DHTServers.size(); i++){
			if (address.equals(DHTServers.get(i))) {
				posAddress = i;
				continue;
			}
		}
		return posAddress;
	}

	public int[] getNodes(String key) {
		int pos = getPos(key);
		int[] nodes = new int[nReplica];
		for (int i = 0; i < nodes.length; i++) {
			nodes[i] = (pos + i) % nServersMax;
		}
		return nodes;
	}

	public void addDHT(DHTUserInterface dht, int pos) {
		DHTTables.put(pos, dht);
	}

	public DHTUserInterface getDHT(String key) {
		return DHTTables.get(getPos(key));
	}

	public boolean isDHTLocalReplica (String key, String address) { 
		//int pos = getPos(key);
		return address.equals(localAddress);
	}

	public boolean isDHTLocalReplica (int posReplica, String key) { 
		int pos = getPos(key);
		return posReplica == pos;
	}

	public boolean isDHTLocal (int pos) {
		boolean isLocal = localAddress.equals(DHTServers.get(pos));
		LOGGER.fine("Posición: " + pos + ", isDHTLocal: " + isLocal);
		return isLocal;
	}

	public boolean isDHTLocal (String key) {
		int pos = getPos(key);
		boolean isLocal = localAddress.equals(DHTServers.get(pos));
		LOGGER.fine("Posición: " + pos + ", isDHTLocal: " + isLocal);
		return isLocal;
	}

	public String DHTAddress (int pos) {
		//Address aux = DHTServers.get(pos);
		return DHTServers.get(pos);
	}

	public String DHTAddress (String key) {
		// NO REPLICATION!!!!
		int pos = getPos(key);
		return DHTServers.get(pos);
	}
	
	public HashMap<Integer, DHTUserInterface> getDHTTables() {
		return DHTTables;
	}

	java.util.List<String> DHTReplicas (String key) {
		java.util.List<String> DHTReplicas = new java.util.ArrayList<String>();

		int pos = getPos(key);

		if (nReplica > 1) {
			for (int i = 1; i < nReplica; i++) {
				//TODO Si hay fallos podría ser nServersMax
				int aux = (pos + i) % nServersMax; 
				DHTReplicas.add(DHTServers.get(aux));
				LOGGER.fine("Replica #" + aux);
			}
		}
		return DHTReplicas;
	}

	HashMap<Integer, String> getDHTServers() {
		return DHTServers;
	}
	
	public void setDHTServers(HashMap<Integer, String> dhtServers) {
		this.DHTServers= dhtServers;
	}
	
	public String printDHTServers() {
		String aux = "DHTManager: Servers => [";
		for (int i = 0; i < nServersMax; i++) {
			if (DHTServers.get(i) != null) {
				aux = aux + DHTServers.get(i) + " ";
			} else {
				aux = aux + "null ";	
			}	
		}	
		aux = aux + "]";
		return aux;
	}

	@Override
	public String toString() {
		DHTUserInterface dht;
		String aux = "Size: " + DHTTables.size() + " Local server: " + getPosition(localAddress) +"\n";
		aux = aux + printDHTServers() + "\n";

		for (int i = 0; i < nServersMax; i ++) {
			dht = DHTTables.get(i);
			if (dht == null) {
				aux = aux + "Table " + i + ": null" + "\n" ; 
			} else {
				aux = aux + "Table " + i + ": " + dht.toString() + "\n";
			}
		}
		return aux;
	}
}