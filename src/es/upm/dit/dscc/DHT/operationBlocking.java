package es.upm.dit.dscc.DHT;

public class operationBlocking {

	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;
	
	private boolean        waiting = false;
	private OperationsDHT  operation;
	
	public operationBlocking() {
		
	}
	
	public synchronized OperationsDHT sendOperation() {

		if (waiting) {
			LOGGER.severe("Invoke sendOperation while it is waiting");
			System.out.println("Invoke sendOperation while it is waiting");
			return null;
		}
		waiting = true;	
		try {
			while (waiting) {
				wait();
			}			
		} catch (Exception e) {
			LOGGER.severe("Exception: sendOperation()");
			return null;
		}
		LOGGER.fine("Operation: " +  operation.getOperation() + 
				     ". Value: " + operation.getValue() +
				     ". Status: " + operation.getStatus());
		return operation;
	}

	public synchronized void receiveOperation(OperationsDHT  operation) {
		if (!waiting) {
			LOGGER.severe("Invoke sendOperation while no waiting");
			System.out.println("Invoke sendOperation while no waiting");
			return;
		}
		LOGGER.finest("receiveOperation has ben invoked!!");
		waiting = false;
		this.operation = operation;
		notifyAll();
	}
}
