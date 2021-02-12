package es.upm.dit.dscc.DHT;

import java.util.Map;
import java.util.HashMap;
import java.util.Random;

class Tester {
    private java.util.logging.Logger LOGGER = DHTMain.LOGGER;
	
	 
    public void test(DHTManager dht, Integer value) {
    	DHTManager dhtTest = dht;
    	Integer DHTAnswer = null;
    	Integer testAnswer = null;
    	int correcto = 0; 
    	int incorrecto = 0; 
    	int numTests = value;
    	 HashMap<String, Integer> tabla = new HashMap<String, Integer>();
    	tabla.put("Angel", 1);
    	tabla.put("Bernardo", 2);
    	tabla.put("Carlos", 3);
    	tabla.put("Daniel", 4);
    	tabla.put("Eugenio", 5);
    	tabla.put("Zamorano", 6);
    	
    	for(int i = 0; i< numTests;i++) {
    		switch(i) {
    		case (0):
    			
    	    	DHTAnswer = dhtTest.get("Carlos");
    			testAnswer = tabla.get("Carlos");
    			if(DHTAnswer.equals(testAnswer)) {
    				System.out.println("resultado test0 correcto");
    				correcto ++; 
    			}
    			else { incorrecto ++; 
    			} 
    			break; 
    		case (1):
    			DHTAnswer = dhtTest.get("Bernardo");
    			testAnswer = tabla.get("Bernardo");
    			if(DHTAnswer.equals(testAnswer)) {
    				System.out.println("resultado test1 correcto");
    				correcto ++; 
    			}
    			else { incorrecto++; 
    			} 
    			break; 
    		case (2):
    			dhtTest.put(new DHT_Map("Jaime", 7));
    			tabla.put("Jaime", 7);
    			DHTAnswer = dhtTest.get("Jaime");
    			testAnswer = tabla.get("Jaime");
    			if(DHTAnswer.equals(testAnswer)) {
    				System.out.println("resultado test2 correcto");
    				correcto ++; 
    			}
    			else { incorrecto++; 
    			} 
    			break; 
    		case (3):
    			dhtTest.put(new DHT_Map("Javier", 8));
    			tabla.put("Javier", 8);
    			DHTAnswer = dhtTest.get("Javier");
    			testAnswer = tabla.get("Javier");
    			if(DHTAnswer.equals(testAnswer)) {
    				System.out.println("resultado test3 correcto");
    				correcto ++; 
    			}
    			else { incorrecto ++; 
    			} 
    			break; 
    		case (4):
    			dhtTest.remove("Javier");
    			tabla.remove("Javier", 8);
    			System.out.println(dht.toString());
    			testAnswer = tabla.get("Javier");
    			correcto++;
				System.out.println("resultado test4 correcto");

    			break;
    			
    		case (5):
    			dhtTest.remove("Jaime");
    			tabla.remove("Jaime", 7);
    			System.out.println(dht.toString());
    			testAnswer = tabla.get("Jaime");	
				System.out.println("resultado test5 correcto");
    			correcto++;
    			break; 
 		
    		}
    		}
    	
    	
    	if(incorrecto == 0) {
    		System.out.println("Test Correcto. Se superaron todas las pruebas");
    	}
    	else {
    		System.out.println("Test fallido. Algo salió mal");

    	}
    	System.out.println(" - Correct operations: " + String.valueOf(correcto));
        System.out.println(" - Failed operations: " + String.valueOf(incorrecto));
    	
    	
    	

    }
 }
