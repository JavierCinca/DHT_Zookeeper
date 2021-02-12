package es.upm.dit.dscc.DHT;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

public interface DHTUserInterface {

	Integer put(DHT_Map map);
	
	Integer putMsg(DHT_Map map);

	Integer get(String key);
	
	Integer getMsg(String key);

	Integer remove(String key);

	Integer removeMsg(String key);

	
	boolean containsKey(String key);

	Set<String> keySet();
	
//	Set<Map.Entry<String,Integer>> entrySet();

	ArrayList<Integer> values();

}