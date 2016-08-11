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
