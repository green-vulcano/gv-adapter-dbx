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
import java.sql.SQLException;
import java.util.Objects;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.w3c.dom.Node;

import it.greenvulcano.configuration.XMLConfig;
import it.greenvulcano.gvesb.buffer.GVBuffer;
import it.greenvulcano.gvesb.channel.jdbc.JdbcChannel;
import it.greenvulcano.gvesb.virtual.CallOperation;
import it.greenvulcano.gvesb.virtual.InitializationException;
import it.greenvulcano.gvesb.virtual.OperationKey;

public abstract class JdbcCallOperation implements CallOperation {

	protected static final Logger logger = org.slf4j.LoggerFactory.getLogger(JdbcCallOperation.class);
	private OperationKey key = null;      
	
	
	protected String username = null;
	protected String password = null;
	protected String statement = null;
	
	private DataSource datasource;
	
	@Override
	public void init(Node node) throws InitializationException {
	
		try {
			username = XMLConfig.get(node, "@username", null);
	        password = XMLConfig.getDecrypted(node, "@username", null);        
	        
	        statement = Objects.requireNonNull(node.getTextContent(), "Missing SQL statment")
	        				   .replaceAll("\n", " ")
	        				   .replaceAll("\t", " ")
	        				   .replaceAll("\r", " ")
	        				   .replaceAll("\\s+", " ")
	        				   .trim();

	        String systemId = XMLConfig.get(node.getParentNode().getParentNode(), "@id-system", null);
	        String channelId = XMLConfig.get(node.getParentNode(), "@id-channel", null);        
        
	        datasource = Objects.requireNonNull(JdbcChannel.getDatasource(systemId, channelId),"Datasource not found on "+systemId+"/"+channelId);
		 } catch (Exception exc) {
	            throw new InitializationException("GV_INIT_SERVICE_ERROR", new String[][]{{"message", exc.getMessage()}},
	                    exc);
	     }
    }
			
	protected Connection getConnection() throws SQLException{
		return  (username!=null) ? datasource.getConnection(username, password) : datasource.getConnection();
	}

	@Override
	public void cleanUp() {
		
	}

	@Override
	public void destroy() {
		
	}

	@Override
    public String getServiceAlias(GVBuffer gvBuffer) {
        return gvBuffer.getService();
    }


    /**
     * @see it.greenvulcano.gvesb.virtual.Operation#setKey(it.greenvulcano.gvesb.virtual.OperationKey)
     */
    @Override
    public void setKey(OperationKey key) {
        this.key = key;
    }

    /**
     * @see it.greenvulcano.gvesb.virtual.Operation#getKey()
     */
    @Override
    public OperationKey getKey() {
        return key;
    }	
}
