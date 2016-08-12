/*******************************************************************************
 * Copyright (c) 2009, 2016 GreenVulcano ESB Open Source Project.
 * All rights reserved.
 *
 * This file is part of GreenVulcano ESB.
 *
 * GreenVulcano ESB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * GreenVulcano ESB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with GreenVulcano ESB. If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
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