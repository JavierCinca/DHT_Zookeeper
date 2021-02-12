package es.upm.dit.dscc.DHT;

import java.io.Serializable;

public class DHT_Map implements Serializable{
	
	private static final long serialVersionUID = 2L;
	private String  key;
	private Integer value;

	public DHT_Map (String key, Integer value ) {
		this.key   = key;
		this.value = value;
	}
	
	public String getKey() {
		return this.key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public int getValue() {
		return this.value;
	}
	public void setValue(Integer value) {
		this.value = value;
	}

	@Override
	public String toString() {
		//return "DHT_map [key=" + key + ", value=" + value + "]";
		return "(" + key + " ," + value + ")";
	}

	
	
}
