package es.upm.dit.dscc.DHT;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class zkOp implements Watcher{
	
	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;
	
	//Nodo raiz
	private static String operations = "/operations";
	private static String operacion = "/oper-"
	//Operaciones
;
	private String IdentificadorOperacion;
	private String rutaOp;
	private operationBlocking opBlock;	
	private String respuesta = new String();
	private String actualOpe = new String(); 
	private DataHandler rData;
	String localHost= "127.0.0.1:2181";
	private ZooKeeper zk;	
	
	public zkOp (byte[] data, operationBlocking opBlock) {		
		this.opBlock = opBlock;
		// Comprobamos si hay una sesion de zookeeper, sino esperamos a que se cree. 
		try {
			if (zk == null) {
				zk = new ZooKeeper(localHost, 5000, SessionWatcher);
				try {
					//Esperamos a que zookeeper tenga una session activa.
					wait();
				} catch (Exception e) {
					LOGGER.fine(" MIRAR clase zkOp. Excepcion : "+e);
				}
			}
		} catch (Exception e) {
			System.out.println("Error zkOp esperando zk");
		}
		
		if (zk != null) {
			try {	
				//Creamos Znode operacion si no existe. 
				Stat stat = zk.exists(operations, false); 
				if (stat == null) {
					respuesta = zk.create(operations, new byte[0],Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(respuesta);
				}
				//Cada vez que se llame al metodo creamos un hijo efimero.
				rutaOp = zk.create(operations + operacion, data, 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				getDatos(rutaOp);
				IdentificadorOperacion = rutaOp.replace(operations + "/", "");
				List<String> list = zk.getChildren(operations, false, stat);
				System.out.println("Creada el id de la operacion en el znode:"+ IdentificadorOperacion );
				System.out.println("Datos del nodo zOperation: " + rData.toString());
				displayOperation(list);

			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}

		}
	}	
	// Notifica cuando se crea la sesion
	private Watcher SessionWatcher = new Watcher() {
		public void process (WatchedEvent e) {
			System.out.println("Created session");
			//System.out.println(e.toString());
			notify();
		}
	};

	
	// Muestra cuando el numero de hijos se han actualizado
	private Watcher  watcherOperation = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("<<<<<<<<<<<<<   Watcher Operation  >>>>>>>>>>>>>\n");		
			try {	
				List<String> list = zk.getChildren(operations,  false); //this);
				displayOperation(list);			
				System.out.println("Parece que todo ha ido bien");				
			} catch (Exception e) {
				System.out.println("Excepcion en el wacherMember: " + e);
			}
		}
	};
	
	// Notifica cuando los datos han sido actualizados
	private Watcher  watcherData = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("<<<<<<<<<<<<<   Watcher Data   >>>>>>>>>>>>>\n");		
			try {

				int[] answer = getDatos(rutaOp).getRespuesta();
					//Cuando ambos servidores han ejecutado la operacion
				boolean OperationDone = auxWatcher(answer);
				if(OperationDone) {
					OperationsDHT operation = rData.getOperacion();
					opBlock.receiveOperation(operation);
					//Borramos el nodo op cuando se ha realizado. Lanzamos un watcher.
					Stat stat = zk.exists(rutaOp, false);
					zk.delete(rutaOp, stat.getVersion());
				}else {
					
				}
				
			} catch (Exception e) {
				System.out.println("Exception en el wacherData: "+ e);
			}
		}
	};
	@Override
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
			List<String> list = zk.getChildren(operations, false);
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}	
	
	
	//----------------------------------------------------------------------------------------
		
	private boolean auxWatcher(int[] answer) {
		boolean operationCheck = true;
		for(int i= 0; i<answer.length; i++) {
			if(answer[i]==0) {
				operationCheck = false;
				LOGGER.warning("watcherData: El servidor" + i +" no ha hecho todavia la operacion");
			}
		}	//Cuando ambos servidores han ejecutado la operacion	
		
		return operationCheck;
		
	}
	private void displayOperation(List<String> list) {
		System.out.println("Remaining # members:" + list.size());
		for(String s : list) {
			 actualOpe = actualOpe + s;					
		}
		System.out.println("--------------------------------------");
		System.out.println("Operacion actual ejecutandose: " + actualOpe);
		System.out.println("--------------------------------------");
		System.out.println();	
	}
	
	public DataHandler getDatos (String pathOp) throws KeeperException{
		try {
			Stat opStat = zk.exists(pathOp, false);
			byte[] opData = zk.getData(pathOp, watcherData, opStat);			
			rData = Serializator.deserialize(opData);	

		}
		catch (Exception e) {
			System.out.println("Exception obteniendo datos: "+ e);
		}
		return rData;

	
	}
	


	
	
}