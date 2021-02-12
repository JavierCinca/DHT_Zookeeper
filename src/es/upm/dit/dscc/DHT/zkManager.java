package es.upm.dit.dscc.DHT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import java.util.Random;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class zkManager implements Watcher {

	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;
	private int nReplica;
	private int servNumMax;
	
	private int servNum;
	private List<Stat> stats = new ArrayList<Stat>();
	private boolean isQuorum = false;
	private boolean setOK= false;
	private boolean wasQuorum = false;
	private List<String> oldServers = null;
	private String localAddress;
	private TableManager tableManager;
	private DHTUserInterface dht;

	
	//DATA
	private HashMap<Integer, String> znodesCreados;
	
	
	//RUTAS ZOOKEEPER
	private String[]  nodes= { servers, servers + serverData, operations, lock, tables, tables + table0, tables + table1, tables + table2}; 
	private String[]  nodesreset= { servers + serverData, servers, operations, lock, tables + table0, tables + table1, tables + table2, tables }; 

	//Servidores
	private static final int SESSION_TIMEOUT = 5000;
	private static String servers = "/severs";
	private static String serv = "/serv-";
	private String identifier;
	private Integer position;
	//OPERATIONS
	private static String operations = "/operations";
	//LOCK
	private static String lock = "/lock";
	private static String opLock = "/opok-";
	private static Integer mutex = -1;
	private String lockId;
	private static String lockLeader;
	//DATA
	private static String serverData = "/serverdata";
	private static String tables="/tables";
	private static String table0 = "/table0";
	private static String table1 = "/table1";
	private static String table2 = "/table2";
	private static String[] tableList = {tables + table0, tables + table1, tables + table2};

	//ZOOKEEPER
	private ZooKeeper zk;
	String[] hosts = { "127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181" };

	//JAR PATH
	
	private String scriptPath = System.getProperty("user.dir") + "/scripts/relanzaServerCaido.sh";

	//CONSTRUCTOR
	public zkManager(int servNumMax, int nReplica, TableManager tableManager, DHTUserInterface dht) {
		this.servNum = 0;
		this.servNumMax = servNumMax;
		this.nReplica = nReplica;
		this.tableManager = tableManager; 
		this.dht = dht;
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);

		//INICIACION DEL SISTEMA
		try {
			if (zk == null) {
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, cWatcher);
				try {
					wait();
				} catch (Exception e) {
				}
				Stat estado = zk.exists(servers, false);
				
				//Lamada a reset si no hay ningun Znode. Reset resetea el estado del sistemas para dejarlo limpio.
				List<String> list = zk.getChildren(servers, false, estado);
				if (list.size()==1) {
					reset(zk);
				}	
			}
		} catch (Exception e) {
			System.out.println("Try again. Error iniciando proyecto.");
		}
		

		// Configuracion del Cluster de Zookeeper. Creacion de los Znodes 
		if (zk != null) {
			try {
							
				//creamos si no existen los nodos del array nodes
				System.out.println("¿Existen los znodes? ");
				int size = stats.size();
				if(size == 0){
					for(int l = 0; l< nodes.length; l++){
						Stat stat = zk.exists(nodes[l], false);
						System.out.println("¿Existe znode?: " +nodes[l]);
						stats.add(stat);
						
					}
					//System.out.println("Presentamos stats:"+ stats);
					//CREAMOS LOS NODOS PERSISTENTES
					//En stats se gusrdan los estados de los znodes de nodes y si no existen se crean.
					if(stats.get(0) ==null && stats.get(1) == null && stats.get(2) == null && stats.get(3) == null && stats.get(4) == null && stats.get(5) == null && stats.get(6) == null && stats.get(7) == null ){
						System.out.println("No existian!! Pasamos a crearlos ");
						for(int m = 0; m<nodes.length; m++){
							System.out.println("Crea node: " + nodes[m]);
							zk.create(nodes[m], new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}

					} else {
						this.znodesCreados = getServers();
						HashMap<Integer, String> DHTServers = tableManager.getDHTServers();
						HashMap<Integer, DHTUserInterface> DHTTables = tableManager.getDHTTables();
						for (int p = 0; p < servNumMax; p++) {
							if (znodesCreados.get(p) != null) {
								DHTServers.put(p,znodesCreados.get(p));
								DHTTables.put(p, new DHTHashMap());
								this.servNum++;
							}
						}
						LOGGER.fine(tableManager.printDHTServers());
				}
				
				}
				//Creamos el nodo hijo y guardamos un identificador para operaciones posteriores.
				identifier = zk.create(servers + serv, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				identifier = identifier.replace(servers + "/", "");
				this.localAddress = identifier;
				this.tableManager.setLocalAddress(identifier);
				
				//manageZnode metemos un nuevo znode a las tablas.
				List<String> al = zk.getChildren(servers, false, stats.get(0));
				int index = al.size() - 1; 	
				
				Collections.sort(al);
		        // Delete last element by passing index 
		        al.remove(index);		  
		        
				serversManager(al);
				
				position = tableManager.getPosition(identifier);
				
				//Si existen los tres servidores, guardamso las tablas para porder recuperarlas mas adelante. 
				if (servNum == 3) {					
					HashMap<Integer, DHTUserInterface> DHTTables = tableManager.getDHTTables();
					HashMap<Integer, DHTUserInterface> table0 = null;
					HashMap<Integer, DHTUserInterface> table1 = null; 
					HashMap<Integer, DHTUserInterface> table2 = null;
					
					
					for(int j = 0; j< tableList.length; j++){
						if(j==0){
							System.out.println("Creamos la primera tabla");
							HashMap<Integer, DHTUserInterface> newtable0 = getTables(tableList[j]);
						    table0 = newtable0;
						} else if (j==1){
							System.out.println("Creamos la segunda tabla");
							HashMap<Integer, DHTUserInterface> newtable1 = getTables(tableList[j]);
						    table1 = newtable1;
 
						}else if (j==2){
							System.out.println("Creamos la tercera tabla");
							HashMap<Integer, DHTUserInterface> newtable2 = getTables(tableList[j]);
							table2 = newtable2;
						} else 
						return; 
						
					}
					if (table2 == null) {
						table2 = new HashMap<Integer, DHTUserInterface>();
					} else {
						if (position==0) {
							DHTTables.put(0, table0.get(0));
							DHTTables.put(2, table2.get(2));
						} else if (position==1) {
							DHTTables.put(0, table0.get(0));
							DHTTables.put(1, table1.get(1));
						} else {
							DHTTables.put(1, table1.get(1));
							DHTTables.put(2, table2.get(2));
						}
					}
				}
				
				
				
				
				
				//Watcher gestion de servidores
				
				List<String>list = zk.getChildren(servers, watcherServer, stats.get(0));
								System.out.println(">>>>>>>>>>>       Nuevo Servidor en el entorno Zookeeper " + identifier + "<<<<<<<<<<<<<");
				System.out.println("Znodes hijos de servers:  " + list.size());
				muestraZnodes(list);
			
				
				//Solo lo ejecuta el primer servidor del cluster para actualizar los backups
				if (isLeader()) {
					setServers(tableManager.getDHTServers());
				}
				if (position==0) {
					setTables(tableManager.getDHTTables(), tableList[0]);
				} else if (position==1) {
					setTables(tableManager.getDHTTables(), tableList[1]);
				} else {
					setTables(tableManager.getDHTTables(), tableList[2]);
				}
				
				//Watcher sobre las operaciones
				Stat stat2 = zk.exists(operations, false);
				List<String> opList = zk.getChildren(operations, watcherOperation, stat2);
				System.out.println(" Zookeeper operations:" + opList.size());
				muestraZnodes(opList);
				
				
				LOGGER.fine(tableManager.printDHTServers());
				
				
				
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing. At Start");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}
		}
	}
	
	//Metodo para resetear el estado del zookeeper
	private void reset(ZooKeeper zk) {
		try {
			System.out.println("Reseting Znodes...");
			for (int j= 0; j<nodesreset.length; j++) {
				
				Stat s = zk.exists(nodesreset[j],false);
				
				if (s != null) {
					zk.delete(nodesreset[j],s.getVersion());
					System.out.println("----- node: " + nodesreset[j]);
					System.out.println("state: " + s);
				}				
				
			}
			
		} catch (Exception e) {
			LOGGER.warning("ERROR during reset. CHECK RESET");	
		}	
	}   
	
	// Asignamos el identifier a LocalAddress
	public String getLocalAddress() {
		return this.localAddress;
	}

	// Notified when the session is created
	private Watcher cWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			System.out.println("Created session");
			notify();
		}
	};

	@Override
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}

	private void muestraZnodes(List<String> list) {
		String znodes = new String();
		Collections.sort(list);
		for (String s : list) {
			 znodes = znodes + s +", ";
		}
		System.out.println(znodes);
		System.out.println("--------------------------------------------------------------------------------------");
	}
	
	// Notified when the number of children in /member is updated
	private Watcher watcherServer = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("------------------------------------Watcher Member------------------------------------\n");
			try {
				List<String> al = zk.getChildren(servers, watcherServer);
				int index = al.size() - 1; 	
				
				Collections.sort(al);
		        // Delete last element by passing index 
		        al.remove(index);	
		       
		        System.out.println("Modified ArrayList : " + al); 
				serversManager(al);
				if (isLeader()) {
					setServers(tableManager.getDHTServers());
				}
				if (position==0) {
					setTables(tableManager.getDHTTables(), tableList[0]);
				} else if (position==1) {
					setTables(tableManager.getDHTTables(), tableList[1]);
				} else {
					setTables(tableManager.getDHTTables(), tableList[2]);
				}
				LOGGER.fine(tableManager.printDHTServers());
				List<String> list = zk.getChildren(servers, watcherServer);
				System.out.println("Nodos hijo de Servers: " + list.size());
				muestraZnodes(list);
				System.out.println("--------------------------------------------------------------------------------------");
				System.out.println(">>> Enter option: 1) Put. 2) Get. 3) Remove. 4) ContainKey  5) Values 6) Test 7) Init 0) Exit");
			} catch (Exception e) {
				System.out.println("Exception: watcherServer");
			}
		}
	};
	
	
	

	public boolean serversManager(List<String> newServers) {
		
		
		//SI SE CAE UN SERVIDOR
		if (oldServers != null && newServers.size() < oldServers.size()) {
			LOGGER.warning("Un servidor se ha caido!!! > NO QUORUM");
			Collections.sort(newServers);
			String downServer = servidorCaido(oldServers, newServers);
			borraServer(downServer);
			servNum--;
			isQuorum = false;
			oldServers = newServers;
			// SI SE HA CAIDO UN SERVIDOR
			if (wasQuorum && servNum < 3) {
				if (isLeader()) {
					try {
						System.out.println(">>  Relanzamos Servidor caido ");
						List<String> cmdList = new ArrayList<String>();
					      // adding command and args to the list
					    cmdList.add("sh");
					    cmdList.add(scriptPath);
					    ProcessBuilder pb = new ProcessBuilder(cmdList);
					    pb.start();
					} catch (IOException e) {
						System.out.println("Something went wrong trying to restart the server:  " + e);
					}
				}
			}	
			return false;
			
		} else {
			//SI ESTAMOS DESPLEGANDO UN SERVIDOR

			//  1. EN CASO DE QUE YA HAYA 3 SERVIDORES
			if (newServers.size() > servNumMax) {
				return false;
			}
			//  2. SI SE DESPLEGA UN SERVIDOR NUEVO Y HAY MENOS DE 3
			else {
				// PRIMER SERVIDOR DEL SISTEMA 
				if (servNum == 0 && newServers.size() > 0) {
					for (String direccion : newServers) {
						
						addServer(direccion);
						LOGGER.fine("Nuevo servidor en el sistema!! Servidor : " + direccion );
						LOGGER.fine(" >> Nº de servidores actual: " + servNum);
						HashMap<Integer, String> servers = tableManager.getDHTServers();
						int validos = 0;
						for(int l=0; l < servers.size(); l++) {
							if(servers.get(l)!= null) {
								validos++;
							}
						}
						servNum = validos;
					}
				} else {
					// SE AÑADE UN SERVIDOR NUEVO. ACTUALIZA SUS TABLAS.
					
					Collections.sort(newServers);
					String address = newServers.get(newServers.size() - 1);
					addServer(address);
					LOGGER.fine("Nuevo servidor en el sistema!! Servidor : " + address +" >> Nº de servidores actual: " + servNum);
					if (servNum == servNumMax) {
						isQuorum = true;
						System.out.println(">>>>>>>>>    HAY QUORUM!   <<<<<<<<<<");
						System.out.println("       (Podemos hacer operaciones)      ");
						// A server crashed and is a new one
						if (wasQuorum) {
							HashMap<Integer, String> DHTServers;
							String downServer = newServers.get(newServers.size() - 1);
							DHTServers = addServer(downServer);
							if (DHTServers == null) {
								LOGGER.warning("DHTServers is null!!");
							}
							
						} else {
							wasQuorum = true;
						}
					}
				}
			}
		}
		oldServers = newServers;
		return true;
	}

	public HashMap<Integer, String> addServer(String address) {
		
		HashMap<Integer, String> DHTServers = tableManager.getDHTServers();
		HashMap<Integer, DHTUserInterface> DHTTables = tableManager.getDHTTables();
		//en caso de que ya haya 3 servidores pasa
		if (servNum >= servNumMax) {
			return null;
		} else {
			//Comprobamos si existe
			for (int i = 0; i < servNumMax; i++) {
				String server = DHTServers.get(i);
				if (server != address && server == null) {
					DHTServers.put(i, address);
					if (DHTTables.get(i) == null) {
						DHTTables.put(i, new DHTHashMap());
					}
					servNum++;
					return DHTServers;
				}
			}
		
		}
		
		LOGGER.warning("Error: This sentence shound not run");
		return null;
	}
	public Integer borraServer(String address) {
		HashMap<Integer, String> DHTServers = tableManager.getDHTServers();
		for (int i = 0; i < servNumMax; i++) {
			if (address.equals(DHTServers.get(i))) {
				DHTServers.remove(i);
				return i;
			}
		}
		return null;
	}

	public String servidorCaido(List<String> oldServers, List<String> newServers) {
		for (int k = 0; k < newServers.size(); k++) {
			if (oldServers.get(k).equals(newServers.get(k))) {
			} else {
				return oldServers.get(k);
			}
		}
		return oldServers.get(oldServers.size() - 1);
	}
		
	
	
	
	public boolean isQuorum() {
		return isQuorum;
	}

	private boolean isLeader() {
		try {
			List<String> candidatos = zk.getChildren(servers, false);
			int index = candidatos.size() - 1; 				  
	        // Delete last element by passing index 
	        candidatos.remove(index);
			Collections.sort(candidatos);
			int index2 = candidatos.indexOf(identifier.substring(identifier.lastIndexOf('/') + 1));
			if (index2 == 0) {
				System.out.println("I am the leader: " + identifier);
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			System.out.println("Exception: selectLeaderWatcher");
		}
		return false;
	}
		
	
	
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////                                                  OPERACIONES Y LOCKS                                                   //////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	
	private void lockManager() {
		if (zk != null) {
			// Create a folder for locknode and include this process/server
			try {
				Stat s = zk.exists(lock, false); //this);
				// Set watcher on lock
				List<String> list = zk.getChildren(lock, false, s);
				System.out.println(" Zookeeper Locks:" + list.size());
				muestraZnodes(list);
				// Create a znode for registering as member and get my id
				// Deberia activar el watcher creado anteriormente
				lockId = zk.create(lock + opLock, new byte[0], 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				lockId = lockId.replace(lock + "/", "");
				System.out.println("Created znode lock id:"+ lockId );
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing. lockManagers");
				System.out.println("Exception: "+ e);
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
				System.out.println("Exception: "+ e);
			}
		}
		
	}

	
	
	
	
	private boolean operationManager() {
		Integer intentos = 0;		
		
		try {
			List<String> locks = zk.getChildren(lock,  false);
			Collections.sort(locks);
			//Coge la posicion y si no existe devuelve -1
			int index = locks.indexOf(lockId.substring(lockId.lastIndexOf('/') + 1));
			
			String leader = locks.get(0);
			lockLeader = lock + "/" + leader;
			if(index == 0) {
				//Es el lider
				System.out.println("Soy el lider, obtengo el zkOp del proceso, Lock: " + lockId );
				
				//Si el lock es lider, hace la operacion.
				
				try {
					List<String> list = zk.getChildren(operations, false);
					Collections.sort(list);
					String operationPath = operations + "/" + list.get(0);;
					LOGGER.fine("The operation path is: " + operationPath);
					boolean doneOperation = false;
					Stat s = zk.exists(operationPath, false);
					byte[] data= null;
					if (s != null) {
						byte[] datos = zk.getData(operationPath, false, s);
						data = datos;
					}
					
					DataHandler deserializedData = Serializator.deserialize(data);
					int[] zNodes = deserializedData.getServidores();
					
					for(int i = 0; i<zNodes.length;i++) {
						if(zNodes[i]== position) {
							doneOperation = true;
						}
					}
					if(doneOperation) {
						OperationsDHT operation = deserializedData.getOperacion();
						int value=0;
						String key = "";
						OperationEnum opType = operation.getOperation();
						switch(opType) {
						case PUT_MAP:
							DHT_Map map = operation.getMap();
							value = dht.putMsg(map);
							operation.setValue(value);
							
							break;
						case GET_MAP:
							key = operation.getKey();
							value = dht.getMsg(key);
							operation.setValue(value);
							break;
						case REMOVE_MAP:
							key = operation.getKey();
							value = dht.removeMsg(key);
							operation.setValue(value);
							break;
							
						default:
							break;
						}
						
						
						int[] respuesta = deserializedData.getRespuesta();
						// Metemos el valor en el primer valor vacio de respuesta
						for(int i = 0; i< respuesta.length; i++) {
							if(respuesta[i] == 0) {
								respuesta[i] = value;
								LOGGER.fine("La respuesta en la posicion " + i + "  es : " + respuesta[i]);
								break;
							}
						}
						
						// Actualizamos la operacion y la respuesta
						deserializedData.setOperacion(operation);
						deserializedData.setRespuesta(respuesta);
						// Actualizamos el nodo de la operacion, llamará watcher en zkOp
						byte[] updatedData = Serializator.serialize(deserializedData);
						s = zk.exists(operationPath, false);
						zk.setData(operationPath, updatedData, s.getVersion());
					}
					
				} catch (Exception e) {
					System.out.println("Exception: selectLeaderWatcher");
				}
				
				//Borramos el nodo lock una vez hecha la operacion.
				Stat s = zk.exists(lockLeader, false);
				zk.delete(lockLeader, s.getVersion());
				LOGGER.finest("Lock lider borrado: " + lockLeader);
				
				return true;
			} else {
				//no es lider. Esperamos a que el lider termine la operacion.
				Stat s = zk.exists(lockLeader, watcherLock);
				System.out.println("NO soy el lider espero a que lock: " + leader + " acabe.");				 
			
				do{					
					operationManager();
					intentos++;
				}while(intentos < 2);
			
				return false;
			}
		} catch (Exception e) {
			//System.out.println("Exception: lock no es lider");
			//System.out.println("Exception: "+ e);
			return false;
		}
		
	}
	
	// Notified when the number of children in /operations is updated
		private Watcher watcherOperation = new Watcher() {
			public void process(WatchedEvent event) {
				
				try {
					List<String> list = zk.getChildren(operations, watcherOperation); // this);
					
					if (list.size()>0) {
						System.out.println("<<<<<<<<<<<<<<<<<<<  Watcher Operation  >>>>>>>>>>>>>>>>>>> \n");
						System.out.println("¿Cuantas operaciones quedan? ");
						System.out.println("    >>Quedan: " + list.size());
						muestraZnodes(list);
						lockManager();
						operationManager();
						LOGGER.finest("Watcher operation finalizado");
					}else {
						if (position==0) {
							setTables(tableManager.getDHTTables(), tableList[0]);
						} else if (position==1) {
							setTables(tableManager.getDHTTables(), tableList[1]);
						} else {
							setTables(tableManager.getDHTTables(), tableList[2]);
						}
						System.out.println(">>>>>>>>>>>>>>>>>>>>>     Operacion Acabada <<<<<<<<<<<<<<<<<<<<< \n");
						System.out.println(">>> Enter option: 1) Put. 2) Get. 3) Remove. 4) ContainKey  5) Values 6) Test 7) Init 0) Exit");	
					}
					
				} catch (Exception e) {
					System.out.println("Exception: watcherOperation");
				}
			}
		};
			
	// Notified when the number of children in /locknode is updated
	private Watcher  watcherLock = new Watcher() {
		public void process(WatchedEvent event) {
				
			try {
				System.out.println(" Lock Actualizado!!");
				
				// Al recibir el watcher de cualquier nodo notifico a mi hebra de que levante el bloqueo
				synchronized (mutex) {
					mutex.notify();
				}
				List<String> list = zk.getChildren(lock,  false);
				System.out.println("¿Cuantas lock quedan? ");
				System.out.println("    >>Quedan: " + list.size() + " Locks");
				muestraZnodes(list);
			} catch (Exception e) {
				System.out.println("Exception: watcherLock");
				//System.out.println("Exception: " + e);
			}
		}
	};
	
	
	////////////////////////////////////////////////////////////////////////////////////////////////
									//Metodos para la coherencia de zookeeper 
	///////////////////////////////////////////////////////////////////////////////////////////////
	
	
	///// SETTERS
	
	public void setServers(HashMap<Integer, String> DHTServers) {
			byte[] byteDHTServers = null;
			try {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(DHTServers);
				oos.flush();
				byteDHTServers = bos.toByteArray();
				setOK= true;
			
			} catch (Exception e) {
				System.out.println("Error: setServers");
				System.out.println("Error while serializing object");
				System.out.println("Exception: " + e);
			}
			if(setOK) {
				try {
					Stat s = zk.exists(servers + serverData, false);
					zk.setData(servers + serverData, byteDHTServers, s.getVersion());
				} catch (Exception e) {
					System.out.println("Error: setServers");
					System.out.println("Error while setting data to ZK");
					System.out.println("Exception: " + e);
			    }
			}
		}

		

		public void setTables(HashMap<Integer, DHTUserInterface> DHTTables, String path) {
			byte[] byteDHTTables = null;
			try {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(DHTTables);
				oos.flush();
				byteDHTTables = bos.toByteArray();
				setOK= true;
			} catch (Exception e) {
				System.out.println("Error: setTables");
				System.out.println("Error while serializing object");
				System.out.println("Exception: " + e);
			}
			if(setOK) {
				try {
					Stat s = zk.exists(path, false);
					zk.setData(path, byteDHTTables, s.getVersion());
				} catch (Exception e) {
					System.out.println("Error: setTables");
					System.out.println("Error while setting data to ZK");
					System.out.println("Exception: " + e);
				}
			}
		}
		
		///// GETTERS
		
		public HashMap<Integer, String> getServers() {
			Stat s = new Stat();
			byte[] data = null;
			HashMap<Integer, String> DHTServers = new HashMap<Integer, String>();
			try {			
				s = zk.exists(servers + serverData, false);				
				data = zk.getData(servers + serverData, false, s);
				
			} catch (Exception e) {
				System.out.println("Error: getServers");
				System.out.println("Error while getting data from ZK");
				System.out.println("Exception: " + e);
			}
			// Deserialize: Convert an array of Bytes in an operation.
			if (data != null) {
				try {
					ByteArrayInputStream bis = new ByteArrayInputStream(data);			
					ObjectInputStream in = new ObjectInputStream(bis);
					DHTServers = (HashMap<Integer, String>) in.readObject();
				} catch (Exception e) {
					System.out.println("Error: getServers");
					System.out.println("Error while deserializing object");
					System.out.println("Exception: " + e);
				}
			}
			return DHTServers;
		}
		public HashMap<Integer, DHTUserInterface> getTables(String path) {
			Stat stat = new Stat();
			byte[] data = null;
			HashMap<Integer, DHTUserInterface> DHTTables = new HashMap<Integer, DHTUserInterface>();
			try {
				stat = zk.exists(path, false);
				data = zk.getData(path, false, stat);
			} catch (Exception e) {
				
			}
			// Deserialize: Convert an array of Bytes in an operation.
			if (data != null) {
				try {
					ByteArrayInputStream bis = new ByteArrayInputStream(data);
					ObjectInputStream in = new ObjectInputStream(bis);
					DHTTables = (HashMap<Integer, DHTUserInterface>) in.readObject();
				} catch (Exception e) {
					//System.out.println("Error: getTables");
					//System.out.println("Error while deserializing object");
					//System.out.println("Exception: " + e);
					return null;
				}
			}
			return DHTTables;
		}


}