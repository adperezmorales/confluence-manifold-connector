package org.apache.manifoldcf.authorities.authorities.confluence;

/**
 * <p>
 * ConfluenceConfiguration class
 * </p>
 * <p>
 * Class used to keep configuration parameters for Confluence authority connection
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 *
 */
public class ConfluenceConfiguration {

	public static interface Server {
		public static final String USERNAME = "username";
		public static final String PASSWORD = "password";
		public static final String PROTOCOL = "protocol";
		public static final String HOST = "host";
		public static final String PORT = "port";
		public static final String PATH = "path";
		
		public static final String PROTOCOL_DEFAULT_VALUE = "http";
		public static final String HOST_DEFAULT_VALUE = "";
		public static final String PORT_DEFAULT_VALUE = "8090";
		public static final String PATH_DEFAULT_VALUE = "/rpc/json-rpc/confluenceservice-v2/";
		public static final String USERNAME_DEFAULT_VALUE = "";
		public static final String PASSWORD_DEFAULT_VALUE = "";
	}

}
