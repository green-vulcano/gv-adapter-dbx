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
package it.greenvulcano.gvesb.channel.jdbc;

import java.util.Optional;
import java.util.stream.IntStream;

import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import it.greenvulcano.configuration.XMLConfig;
import it.greenvulcano.configuration.XMLConfigException;
import it.greenvulcano.gvesb.virtual.OperationFactory;
import it.greenvulcano.gvesb.virtual.jdbc.ExecutionCallOperation;
import it.greenvulcano.gvesb.virtual.jdbc.QueryCallOperation;

public class Activator implements BundleActivator {
	
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static final String JNDI = "jndi://";
	public static final String OSGI = "osgi://";
	
    public void start(BundleContext context) {
    	logger.debug("Starting bundle GVESB DB Channel");    	
    	OperationFactory.registerSupplier("query-db-call", QueryCallOperation::new);
    	OperationFactory.registerSupplier("exec-db-call", ExecutionCallOperation::new);
    	
    	try {
	    	NodeList channelList = XMLConfig.getNodeList("GVSystems.xml","//Channel[@type='DBAdapter' and @enabled='true']");
			    	
	    	logger.debug("Found "+ channelList.getLength() + " DB Channel");
			IntStream.range(0, channelList.getLength())
			         .mapToObj(channelList::item)
			         .forEach(node -> retrieveDatasource(context, node));
    	} catch (XMLConfigException configExcpetion) {	
    		logger.error("Configuration error", configExcpetion);
    	}
    }

    
    @SuppressWarnings("rawtypes")
    private void retrieveDatasource(BundleContext context, Node node) {

    
    	
    	Optional<DataSource> ds = Optional.empty();
    	try {
    		
    		String endpoint = XMLConfig.get(node, "@endpoint");
        	String channel = XMLConfig.get(node, "@id-channel");
        	String system = XMLConfig.get(node.getParentNode(), "@id-system");
    		
    		if (endpoint == null || endpoint.trim().isEmpty()) {
    			throw new Exception("Illegal datasource url format. Datasource URL cannot be null or empty.");
    		} else if (endpoint.startsWith(JNDI)) {
    			String jndiName = endpoint.substring(JNDI.length());
    			InitialContext initialContext = new InitialContext();
    			try {
    				ds = Optional.ofNullable((DataSource)initialContext.lookup(jndiName));
    			} finally {
    				initialContext.close();
    			}
    		} else if (endpoint.startsWith(OSGI)) {
    			String osgiFilter = endpoint.substring(OSGI.length());
    			String clazz = null;
    			String filter = null;
    			String[] tokens = osgiFilter.split("/", 2);
    			if (tokens.length > 0) {
    				clazz = tokens[0];
    			}
    			if (tokens.length > 1) {
    				filter = tokens[1];
    			}
    			ServiceReference[] references = context.getServiceReferences(clazz, filter);
    			if (references != null) {
    				@SuppressWarnings("unchecked")
    				DataSource datasource = (DataSource) context.getService(references[0]);
    				ds = Optional.ofNullable(datasource);
    			} else {
    				throw new Exception("Unable to find service reference for datasource: " + clazz + "/" + filter);
    			}
    		}
    		
    		if (ds.isPresent()) {
    			JdbcChannel.registerDatasource(ds.get(), system, channel);
    		}
    		
    	} catch (XMLConfigException configExcpetion) {	
    		logger.error("Configuration error", configExcpetion);
    	} catch (Exception e) {
    		logger.error("Error retrieving datasource", e);
    	}

    	
    }
    
    
    public void stop(BundleContext context) {
       
    }

}