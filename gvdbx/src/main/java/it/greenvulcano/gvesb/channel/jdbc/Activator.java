/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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