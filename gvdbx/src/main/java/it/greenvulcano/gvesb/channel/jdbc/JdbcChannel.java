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
