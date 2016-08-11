package it.greenvulcano.gvesb.virtual.jdbc;

import java.sql.Connection;
import java.sql.Statement;
import it.greenvulcano.gvesb.buffer.GVBuffer;
import it.greenvulcano.gvesb.internal.data.GVBufferPropertiesHelper;
import it.greenvulcano.gvesb.virtual.CallException;
import it.greenvulcano.gvesb.virtual.ConnectionException;
import it.greenvulcano.gvesb.virtual.InvalidDataException;
import it.greenvulcano.util.metadata.PropertiesHandler;

public class ExecutionCallOperation extends JdbcCallOperation {
		
	@Override
	public GVBuffer perform(GVBuffer gvBuffer)
			throws ConnectionException, CallException, InvalidDataException, InterruptedException {
		
		try (Connection connection = getConnection()) {
					
		    String query =  PropertiesHandler.expand(statement, GVBufferPropertiesHelper.getPropertiesMapSO(gvBuffer, true), gvBuffer.getObject());;
			
		    try (Statement statement = connection.createStatement()) {
		       		    	
		    	statement.executeUpdate(query);
		       
		    	if (!connection.getAutoCommit()) {
		    		connection.commit();
		    	}
		       
		    }		    
		    
		
		} catch (Exception exc) {
	            throw new CallException("GV_CALL_SERVICE_ERROR", new String[][]{{"service", gvBuffer.getService()},
	                    {"system", gvBuffer.getSystem()}, {"tid", gvBuffer.getId().toString()},
	                    {"message", exc.getMessage()}}, exc);
	    }
		
		
		
		return gvBuffer;
	}

}
