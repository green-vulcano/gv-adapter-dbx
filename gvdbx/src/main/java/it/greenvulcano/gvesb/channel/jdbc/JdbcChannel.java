package it.greenvulcano.gvesb.channel.jdbc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcChannel {
	
	private final static Logger logger = LoggerFactory.getLogger(JdbcChannel.class);
	
	private final static Map<String, DataSource> datasources = new ConcurrentHashMap<>() ;
		
	public static void registerDatasource(DataSource datasource, String system, String id) {			
		logger.debug(String.format("GVESB registering DB channel %s/%s ", system, id));		
		datasources.put(system+":"+id, datasource);
	}
	
	public static DataSource getDatasource(String system, String id) {
		return datasources.get(system+":"+id);	
	}
	
	

}
