package es.upm.dit.dscc.DHT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class Serializator {

	private static java.util.logging.Logger LOGGER = DHTMain.LOGGER;

	public Serializator() {
		super();
	}
	public static byte[] serialize(DataHandler data) {
		byte[] serializedData = null;
		
		LOGGER.fine("Serializing data: " + data.toString());
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
		try {
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(data);
			oos.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		serializedData = bos.toByteArray();
		
		return serializedData;
		
	}
	
	public static DataHandler deserialize(byte[] data) {
		LOGGER.fine("Deserializing data: " + data.toString());
		DataHandler operation = null;
		// Deserialize: Convert an array of Bytes in an operation.
		if (data != null) {
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(data);
				ObjectInputStream     in = new ObjectInputStream(bis);
				operation  = (DataHandler) in.readObject();
			}
			catch (Exception e) {
				System.out.println("Error while deserializing object");
			}

		}
		return operation;

	}

	
	
}

