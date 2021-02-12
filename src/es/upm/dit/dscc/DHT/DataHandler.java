package es.upm.dit.dscc.DHT;

import java.io.Serializable;
import java.util.Arrays;


public class DataHandler implements Serializable {

	private static final long serialVersionUID = 1L;

	
	private OperationsDHT operacion;
	private int[] servidores;
	private int[] respuesta;
	
	public DataHandler(int nReplica, int[] servidores,OperationsDHT operacion ) {
		super();
		this.respuesta = new int[nReplica];
		this.servidores = servidores;
		this.operacion = operacion;
		
		System.out.println("El orden de la serializacion de datos es: Replica : " + nReplica +", Servidores involucrados:  "   + Arrays.toString(servidores));
		System.out.println("siendo ServidorA: s-"	+ servidores[0] + " y ServidorB: s-"	+ servidores[1] + "Operacion " + operacion);

	}

	/////////////////////////////////////////////////////
	////// 				Geters					   //////
	/////////////////////////////////////////////////////

	
	public int[] getRespuesta() {
		//System.out.println("La respuesta obtenida es: "+ respuesta);
		return respuesta;
	}

	public OperationsDHT getOperacion() {
		//System.out.println("La operacion obtenida es: "+ respuesta);
		return operacion;
	}

	
	/////////////////////////////////////////////////////
	////// 				Seters					   //////
	/////////////////////////////////////////////////////
	
	
	public void setRespuesta(int[] respuesta) {
		//System.out.println("La respuesta instaurada es: "+ (respuesta));
		this.respuesta = respuesta;
	}
	
	public void setOperacion(OperationsDHT operation) {
		//System.out.println("La respuesta instaurada es: "+ respuesta);
		this.operacion = operation;
	}

	public int[] getServidores() {
		System.out.println("La lista de servidores es: "+ Arrays.toString(servidores));
		return servidores;
	}

	

	

	
	
	
	
}
