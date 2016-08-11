package it.greenvulcano.gvesb.virtual.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.json.JSONArray;
import org.json.JSONObject;

import it.greenvulcano.gvesb.buffer.GVBuffer;
import it.greenvulcano.gvesb.internal.data.GVBufferPropertiesHelper;
import it.greenvulcano.gvesb.virtual.CallException;
import it.greenvulcano.gvesb.virtual.ConnectionException;
import it.greenvulcano.gvesb.virtual.InvalidDataException;

import it.greenvulcano.util.metadata.PropertiesHandler;

public class QueryCallOperation extends JdbcCallOperation {

	
	@Override
	public GVBuffer perform(GVBuffer gvBuffer)
			throws ConnectionException, CallException, InvalidDataException, InterruptedException {
		
		try (Connection connection = getConnection()) {
					
		    String query =  PropertiesHandler.expand(statement, GVBufferPropertiesHelper.getPropertiesMapSO(gvBuffer, true), gvBuffer.getObject());;
			
		    try (Statement statement = connection.createStatement()) {
		       
		    	
		    	ResultSet resultSet = statement.executeQuery(query);
		        
		        int columns = resultSet.getMetaData().getColumnCount();
		        List<String> names = new ArrayList<>(columns);
		      		         
		        for (int i=1; i<=columns; i++) {
		        	names.add(resultSet.getMetaData().getColumnLabel(i));
		        }
		        
		        JSONArray queryResult = new JSONArray();
		        
		        while (resultSet.next()) {
		            
		        	/*
		        	 * Stream approach
		        	 * 
		        	 *  JSONObject json = names.stream().collect(JSONObject::new, (obj, key) -> {
									            		try {
										            		 if (key.contains(".")){
										         	    		String[] hieararchy = key.split("\\.");
										         	    		
										         	    		JSONObject child = Optional.ofNullable(obj.optJSONObject(hieararchy[0]))
										         	    								   .orElse(new JSONObject());
										         	    		child.put(hieararchy[1], resultSet.getObject(key));
										         	    		
										         	    		obj.put(hieararchy[0], child);
										         	    	} else {
										         	    		obj.put(key, resultSet.getObject(key));
										         	    	}
									            		} catch (SQLException e) {
									            			
									            		}
									            		 
									            	 } , 
									            	(o,other)-> {});
		        	*/
		           
		            JSONObject obj = new JSONObject();
		            for (String key : names){
		            	 if (key.contains(".")){
		         	    		String[] hieararchy = key.split("\\.");
		         	    		
		         	    		JSONObject child = Optional.ofNullable(obj.optJSONObject(hieararchy[0]))
		         	    								   .orElse(new JSONObject());
		         	    		child.put(hieararchy[1],resultSet.getObject(key));
		         	    		
		         	    		obj.put(hieararchy[0], child);
	         	    	} else {
	         	    		obj.put(key, resultSet.getObject(key));
	         	    	}
		            }		            
		            
		            queryResult.put(obj);	          
		            
		        }
		  
		        gvBuffer.setObject(queryResult.toString());
		    }		    
		    
		
		} catch (Exception exc) {
	            throw new CallException("GV_CALL_SERVICE_ERROR", new String[][]{{"service", gvBuffer.getService()},
	                    {"system", gvBuffer.getSystem()}, {"tid", gvBuffer.getId().toString()},
	                    {"message", exc.getMessage()}}, exc);
	    }
		
		
		
		return gvBuffer;
	}	
	
	
}
