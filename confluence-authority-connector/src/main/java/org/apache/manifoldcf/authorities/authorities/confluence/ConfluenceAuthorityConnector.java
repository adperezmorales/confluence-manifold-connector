package org.apache.manifoldcf.authorities.authorities.confluence;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.manifoldcf.authorities.authorities.BaseAuthorityConnector;
import org.apache.manifoldcf.authorities.interfaces.AuthorizationResponse;
import org.apache.manifoldcf.core.interfaces.ConfigParams;
import org.apache.manifoldcf.core.interfaces.IHTTPOutput;
import org.apache.manifoldcf.core.interfaces.IPostParameters;
import org.apache.manifoldcf.core.interfaces.IThreadContext;
import org.apache.manifoldcf.core.interfaces.ManifoldCFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Confluence Authority Connector class
 * </p>
 * <p>
 * ManifoldCF Authority connector to deal with Confluence documents
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 *
 */
public class ConfluenceAuthorityConnector extends BaseAuthorityConnector {

	/*
	 * Prefix for Confluence configuration and specification parameters
	 */
	private static final String PARAMETER_PREFIX = "confluence_";

	/* Configuration tabs */
	private static final String CONF_SERVER_TAB_PROPERTY = "ConfluenceRepositoryConnector.Server";

	// pages & js
	// Template names for Confluence configuration
	/**
	 * Forward to the javascript to check the configuration parameters
	 */
	private static final String EDIT_CONFIG_HEADER_FORWARD = "editConfiguration_conf.js";
	/**
	 * Server tab template
	 */
	private static final String EDIT_CONFIG_FORWARD_SERVER = "editConfiguration_conf_server.html";

	/**
	 * Forward to the HTML template to view the configuration parameters
	 */
	private static final String VIEW_CONFIG_FORWARD = "viewConfiguration_conf.html";


	private Logger logger = LoggerFactory
			.getLogger(ConfluenceAuthorityConnector.class);

	/**
	 * <p>
	 * Default constructor
	 * </p>
	 */
	public ConfluenceAuthorityConnector() {
		super();
	}

	

	/**
	 * Close the connection. Call this before discarding the connection.
	 */
	@Override
	public void disconnect() throws ManifoldCFException {
		super.disconnect();

	}

	/**
	 * Makes connection to server
	 * 
	 * 
	 */
	@Override
	public void connect(ConfigParams configParams) {
		super.connect(configParams);
	}

	/**
	 * Checks if connection is available
	 */
	@Override
	public String check() throws ManifoldCFException {
		return super.check();
	}

	

	/**
	 * This method is called to assess whether to count this connector instance
	 * should actually be counted as being connected.
	 *
	 * @return true if the connector instance is actually connected.
	 */
	@Override
	public boolean isConnected() {
		return super.isConnected();
	}


	@Override
	public void viewConfiguration(IThreadContext threadContext,
			IHTTPOutput out, Locale locale, ConfigParams parameters)
			throws ManifoldCFException, IOException {
		Map<String, String> paramMap = new HashMap<String, String>();

		Messages.outputResourceWithVelocity(out, locale, VIEW_CONFIG_FORWARD,
				paramMap, true);
	}

	@Override
	public void outputConfigurationHeader(IThreadContext threadContext,
			IHTTPOutput out, Locale locale, ConfigParams parameters,
			List<String> tabsArray) throws ManifoldCFException, IOException {
		// Add the Server tab
		tabsArray.add(Messages.getString(locale, CONF_SERVER_TAB_PROPERTY));
		// Map the parameters
		Map<String, String> paramMap = new HashMap<String, String>();

		// Output the Javascript - only one Velocity template for all tabs
		Messages.outputResourceWithVelocity(out, locale,
				EDIT_CONFIG_HEADER_FORWARD, paramMap, true);
	}

	@Override
	public void outputConfigurationBody(IThreadContext threadContext,
			IHTTPOutput out, Locale locale, ConfigParams parameters,
			String tabName) throws ManifoldCFException, IOException {

		// Call the Velocity templates for each tab
		Map<String, String> paramMap = new HashMap<String, String>();
		// Set the tab name
		paramMap.put("TabName", tabName);

		// Server tab
		Messages.outputResourceWithVelocity(out, locale,
				EDIT_CONFIG_FORWARD_SERVER, paramMap, true);

	}

	/*
	 * Repository specification post handle, (server and proxy & client secret
	 * etc)
	 * 
	 * @see
	 * org.apache.manifoldcf.core.connector.BaseConnector#processConfigurationPost
	 * (org.apache.manifoldcf.core.interfaces.IThreadContext,
	 * org.apache.manifoldcf.core.interfaces.IPostParameters,
	 * org.apache.manifoldcf.core.interfaces.ConfigParams)
	 */
	@Override
	public String processConfigurationPost(IThreadContext threadContext,
			IPostParameters variableContext, ConfigParams parameters)
			throws ManifoldCFException {

		/* null means process configuration has been successful */
		return null;
	}
	
	  /*
	   * (non-Javadoc)
	   * @see org.apache.manifoldcf.authorities.authorities.BaseAuthorityConnector#getDefaultAuthorizationResponse(java.lang.String)
	   */
	  @Override
	  public AuthorizationResponse getDefaultAuthorizationResponse(String userName) {
	    return RESPONSE_UNREACHABLE;
	  }

	  /*
	   * (non-Javadoc)
	   * @see org.apache.manifoldcf.authorities.authorities.BaseAuthorityConnector#getAuthorizationResponse(java.lang.String)
	   */
	  @Override
	  public AuthorizationResponse getAuthorizationResponse(String userName)
	      throws ManifoldCFException {
	     return super.getAuthorizationResponse(userName);
	  }

}
