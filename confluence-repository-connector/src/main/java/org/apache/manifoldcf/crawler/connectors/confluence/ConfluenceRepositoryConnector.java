package org.apache.manifoldcf.crawler.connectors.confluence;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.manifoldcf.agents.interfaces.RepositoryDocument;
import org.apache.manifoldcf.agents.interfaces.ServiceInterruption;
import org.apache.manifoldcf.core.interfaces.ConfigParams;
import org.apache.manifoldcf.core.interfaces.IHTTPOutput;
import org.apache.manifoldcf.core.interfaces.IPasswordMapperActivity;
import org.apache.manifoldcf.core.interfaces.IPostParameters;
import org.apache.manifoldcf.core.interfaces.IThreadContext;
import org.apache.manifoldcf.core.interfaces.ManifoldCFException;
import org.apache.manifoldcf.core.interfaces.Specification;
import org.apache.manifoldcf.core.interfaces.SpecificationNode;
import org.apache.manifoldcf.crawler.connectors.BaseRepositoryConnector;
import org.apache.manifoldcf.crawler.connectors.confluence.client.ConfluenceClient;
import org.apache.manifoldcf.crawler.connectors.confluence.model.ConfluenceResponse;
import org.apache.manifoldcf.crawler.connectors.confluence.model.Page;
import org.apache.manifoldcf.crawler.interfaces.IExistingVersions;
import org.apache.manifoldcf.crawler.interfaces.IProcessActivity;
import org.apache.manifoldcf.crawler.interfaces.ISeedingActivity;
import org.apache.manifoldcf.crawler.system.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * <p>
 * Confluence Repository Connector class
 * </p>
 * <p>
 * ManifoldCF Repository connector to deal with Confluence documents
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 *
 */
public class ConfluenceRepositoryConnector extends BaseRepositoryConnector {

	protected final static String ACTIVITY_READ = "read document";

	/** Deny access token for default authority */
	private final static String defaultAuthorityDenyToken = GLOBAL_DENY_TOKEN;

	/*
	 * Prefix for Confluence configuration and specification parameters
	 */
	private static final String PARAMETER_PREFIX = "confluence_";

	/* Configuration tabs */
	private static final String CONF_SERVER_TAB_PROPERTY = "ConfluenceRepositoryConnector.Server";

	/* Specification tabs */
	private static final String CONF_SPACES_TAB_PROPERTY = "ConfluenceRepositoryConnector.Space";

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

	// Template names for Confluence job specification
	/**
	 * Forward to the javascript to check the specification parameters for the
	 * job
	 */
	private static final String EDIT_SPEC_HEADER_FORWARD = "editSpecification_conf.js";
	/**
	 * Forward to the template to edit the spaces for the job
	 */
	private static final String EDIT_SPEC_FORWARD_SPACES = "editSpecification_confSpaces.html";

	/**
	 * Forward to the template to view the specification parameters for the job
	 */
	private static final String VIEW_SPEC_FORWARD = "viewSpecification_conf.html";

	protected long lastSessionFetch = -1L;
	protected static final long timeToRelease = 300000L;

	protected final static long interruptionRetryTime = 5L * 60L * 1000L;

	private Logger logger = LoggerFactory
			.getLogger(ConfluenceRepositoryConnector.class);

	/* Confluence instance parameters */
	protected String protocol = null;
	protected String host = null;
	protected String port = null;
	protected String path = null;
	protected String username = null;
	protected String password = null;

	protected ConfluenceClient confluenceClient = null;

	/**
	 * <p>
	 * Default constructor
	 * </p>
	 */
	public ConfluenceRepositoryConnector() {
		super();
	}

	@Override
	public String[] getActivitiesList() {
		return new String[] { ACTIVITY_READ };
	}

	@Override
	public String[] getBinNames(String documentIdentifier) {
		return new String[] { host };
	}

	/**
	 * Close the connection. Call this before discarding the connection.
	 */
	@Override
	public void disconnect() throws ManifoldCFException {
		if (confluenceClient != null) {
			confluenceClient = null;
		}

		protocol = null;
		host = null;
		port = null;
		path = null;
		username = null;
		password = null;

	}

	/**
	 * Makes connection to server
	 * 
	 * 
	 */
	@Override
	public void connect(ConfigParams configParams) {
		super.connect(configParams);

		protocol = params.getParameter(ConfluenceConfiguration.Server.PROTOCOL);
		host = params.getParameter(ConfluenceConfiguration.Server.HOST);
		port = params.getParameter(ConfluenceConfiguration.Server.PORT);
		path = params.getParameter(ConfluenceConfiguration.Server.PATH);
		username = params.getParameter(ConfluenceConfiguration.Server.USERNAME);
		password = params
				.getObfuscatedParameter(ConfluenceConfiguration.Server.PASSWORD);

		try {
			initConfluenceClient();
		} catch (ManifoldCFException e) {
			logger.debug(
					"Not possible to initialize Confluence client. Reason: {}",
					e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Checks if connection is available
	 */
	@Override
	public String check() throws ManifoldCFException {
		try {
			if (!isConnected()) {
				initConfluenceClient();
			}
			Boolean result = confluenceClient.check();
			if (result)
				return super.check();
			else
				throw new ManifoldCFException(
						"Confluence instance could not be reached");
		} catch (ServiceInterruption e) {
			return "Connection temporarily failed: " + e.getMessage();
		} catch (ManifoldCFException e) {
			return "Connection failed: " + e.getMessage();
		} catch (Exception e) {
			return "Connection failed: " + e.getMessage();
		}
	}

	/**
	 * <p>
	 * Initialize Confluence client using the configured parameters
	 * 
	 * @throws ManifoldCFException
	 */
	protected void initConfluenceClient() throws ManifoldCFException {
		if (confluenceClient == null) {

			if (StringUtils.isEmpty(protocol)) {
				throw new ManifoldCFException("Parameter "
						+ ConfluenceConfiguration.Server.PROTOCOL
						+ " required but not set");
			}

			if (Logging.connectors.isDebugEnabled()) {
				Logging.connectors.debug("Confluence protocol = '" + protocol
						+ "'");
			}

			if (StringUtils.isEmpty(host)) {
				throw new ManifoldCFException("Parameter "
						+ ConfluenceConfiguration.Server.HOST
						+ " required but not set");
			}

			if (Logging.connectors.isDebugEnabled()) {
				Logging.connectors.debug("Confluence host = '" + host + "'");
			}

			if (Logging.connectors.isDebugEnabled()) {
				Logging.connectors.debug("Confluence port = '" + port + "'");
			}

			if (StringUtils.isEmpty(path)) {
				throw new ManifoldCFException("Parameter "
						+ ConfluenceConfiguration.Server.PATH
						+ " required but not set");
			}

			if (Logging.connectors.isDebugEnabled()) {
				Logging.connectors.debug("Confluence path = '" + path + "'");
			}

			if (Logging.connectors.isDebugEnabled()) {
				Logging.connectors.debug("Confluence username = '" + username
						+ "'");
			}

			if (Logging.connectors.isDebugEnabled()) {
				Logging.connectors
						.debug("Confluence password '" + password != null ? "set"
								: "not set" + "'");
			}

			int portInt;
			if (port != null && port.length() > 0) {
				try {
					portInt = Integer.parseInt(port);
				} catch (NumberFormatException e) {
					throw new ManifoldCFException("Bad number: "
							+ e.getMessage(), e);
				}
			} else {
				if (protocol.toLowerCase(Locale.ROOT).equals("http"))
					portInt = 80;
				else
					portInt = 443;
			}

			/* Generating a client to perform Confluence requests */
			confluenceClient = new ConfluenceClient(protocol, host, portInt,
					path, username, password);
			lastSessionFetch = System.currentTimeMillis();
		}

	}

	/**
	 * This method is called to assess whether to count this connector instance
	 * should actually be counted as being connected.
	 *
	 * @return true if the connector instance is actually connected.
	 */
	@Override
	public boolean isConnected() {
		return confluenceClient != null;
	}

	@Override
	public void poll() throws ManifoldCFException {
		if (lastSessionFetch == -1L) {
			return;
		}

		long currentTime = System.currentTimeMillis();
		if (currentTime >= lastSessionFetch + timeToRelease) {
			confluenceClient.close();
			confluenceClient = null;
			lastSessionFetch = -1L;
		}
	}

	@Override
	public int getMaxDocumentRequest() {
		return super.getMaxDocumentRequest();
	}

	/**
	 * Return the list of relationship types that this connector recognizes.
	 *
	 * @return the list.
	 */
	@Override
	public String[] getRelationshipTypes() {
		return new String[] {};
	}

	private void fillInServerConfigurationMap(Map<String, String> serverMap,
			IPasswordMapperActivity mapper, ConfigParams parameters) {
		String confluenceProtocol = parameters
				.getParameter(ConfluenceConfiguration.Server.PROTOCOL);
		String confluenceHost = parameters
				.getParameter(ConfluenceConfiguration.Server.HOST);
		String confluencePort = parameters
				.getParameter(ConfluenceConfiguration.Server.PORT);
		String confluencePath = parameters
				.getParameter(ConfluenceConfiguration.Server.PATH);
		String confluenceUsername = parameters
				.getParameter(ConfluenceConfiguration.Server.USERNAME);
		String confluencePassword = parameters
				.getObfuscatedParameter(ConfluenceConfiguration.Server.PASSWORD);

		if (confluenceProtocol == null)
			confluenceProtocol = ConfluenceConfiguration.Server.PROTOCOL_DEFAULT_VALUE;
		if (confluenceHost == null)
			confluenceHost = ConfluenceConfiguration.Server.HOST_DEFAULT_VALUE;
		if (confluencePort == null)
			confluencePort = ConfluenceConfiguration.Server.PORT_DEFAULT_VALUE;
		if (confluencePath == null)
			confluencePath = ConfluenceConfiguration.Server.PATH_DEFAULT_VALUE;

		if (confluenceUsername == null)
			confluenceUsername = ConfluenceConfiguration.Server.USERNAME_DEFAULT_VALUE;
		if (confluencePassword == null)
			confluencePassword = ConfluenceConfiguration.Server.PASSWORD_DEFAULT_VALUE;
		else
			confluencePassword = mapper.mapPasswordToKey(confluencePassword);

		serverMap.put(PARAMETER_PREFIX
				+ ConfluenceConfiguration.Server.PROTOCOL, confluenceProtocol);
		serverMap.put(PARAMETER_PREFIX + ConfluenceConfiguration.Server.HOST,
				confluenceHost);
		serverMap.put(PARAMETER_PREFIX + ConfluenceConfiguration.Server.PORT,
				confluencePort);
		serverMap.put(PARAMETER_PREFIX + ConfluenceConfiguration.Server.PATH,
				confluencePath);
		serverMap.put(PARAMETER_PREFIX
				+ ConfluenceConfiguration.Server.USERNAME, confluenceUsername);
		serverMap.put(PARAMETER_PREFIX
				+ ConfluenceConfiguration.Server.PASSWORD, confluencePassword);
	}

	@Override
	public void viewConfiguration(IThreadContext threadContext,
			IHTTPOutput out, Locale locale, ConfigParams parameters)
			throws ManifoldCFException, IOException {
		Map<String, String> paramMap = new HashMap<String, String>();

		/* Fill server configuration parameters */
		fillInServerConfigurationMap(paramMap, out, parameters);

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

		/* Fill server configuration parameters */
		fillInServerConfigurationMap(paramMap, out, parameters);

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

		// Fill in the parameters
		fillInServerConfigurationMap(paramMap, out, parameters);

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

		String confluenceProtocol = variableContext
				.getParameter(PARAMETER_PREFIX
						+ ConfluenceConfiguration.Server.PROTOCOL);
		if (confluenceProtocol != null)
			parameters.setParameter(ConfluenceConfiguration.Server.PROTOCOL,
					confluenceProtocol);

		String confluenceHost = variableContext.getParameter(PARAMETER_PREFIX
				+ ConfluenceConfiguration.Server.HOST);
		if (confluenceHost != null)
			parameters.setParameter(ConfluenceConfiguration.Server.HOST,
					confluenceHost);

		String confluencePort = variableContext.getParameter(PARAMETER_PREFIX
				+ ConfluenceConfiguration.Server.PORT);
		if (confluencePort != null)
			parameters.setParameter(ConfluenceConfiguration.Server.PORT,
					confluencePort);

		String confluencePath = variableContext.getParameter(PARAMETER_PREFIX
				+ ConfluenceConfiguration.Server.PATH);
		if (confluencePath != null)
			parameters.setParameter(ConfluenceConfiguration.Server.PATH,
					confluencePath);

		String confluenceUsername = variableContext
				.getParameter(PARAMETER_PREFIX
						+ ConfluenceConfiguration.Server.USERNAME);
		if (confluenceUsername != null)
			parameters.setParameter(ConfluenceConfiguration.Server.USERNAME,
					confluenceUsername);

		String confluencePassword = variableContext
				.getParameter(PARAMETER_PREFIX
						+ ConfluenceConfiguration.Server.PASSWORD);
		if (confluencePassword != null)
			parameters.setObfuscatedParameter(
					ConfluenceConfiguration.Server.PASSWORD,
					variableContext.mapKeyToPassword(confluencePassword));

		/* null means process configuration has been successful */
		return null;
	}

	private void fillInConfSpacesSpecificationMap(Map<String, Object> newMap,
			Specification ds) {
		// List<String> spaceKeysList = new ArrayList<String>();
		Optional<String> space = this.getSpaceFromSpecification(ds);

		newMap.put(ConfluenceConfiguration.Specification.SPACE,
				space.isPresent() ? space.get() : "");
	}

	@Override
	public void viewSpecification(IHTTPOutput out, Locale locale,
			Specification ds, int connectionSequenceNumber)
			throws ManifoldCFException, IOException {

		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("SeqNum", Integer.toString(connectionSequenceNumber));

		fillInConfSpacesSpecificationMap(paramMap, ds);

		Messages.outputResourceWithVelocity(out, locale, VIEW_SPEC_FORWARD,
				paramMap);
	}

	/*
	 * Handle job specification post
	 * 
	 * @see org.apache.manifoldcf.crawler.connectors.BaseRepositoryConnector#
	 * processSpecificationPost
	 * (org.apache.manifoldcf.core.interfaces.IPostParameters,
	 * org.apache.manifoldcf.crawler.interfaces.DocumentSpecification)
	 */

	@Override
	public String processSpecificationPost(IPostParameters variableContext,
			Locale locale, Specification ds, int connectionSequenceNumber)
			throws ManifoldCFException {

		String seqPrefix = "s" + connectionSequenceNumber + "_";

		// Delete all preconfigured spaces
		int i = 0;
		while (i < ds.getChildCount()) {
			SpecificationNode sn = ds.getChild(i);
			if (sn.getType().equals(
					ConfluenceConfiguration.Specification.SPACES))
				ds.removeChild(i);
			else
				i++;
		}

		SpecificationNode spaces = new SpecificationNode(
				ConfluenceConfiguration.Specification.SPACES);
		ds.addChild(ds.getChildCount(), spaces);

		String spaceKey = variableContext.getParameter(seqPrefix + "space");
		if (spaceKey != null && !spaceKey.isEmpty()) {
			SpecificationNode node = new SpecificationNode(
					ConfluenceConfiguration.Specification.SPACE);
			node.setAttribute(
					ConfluenceConfiguration.Specification.SPACE_KEY_ATTRIBUTE,
					spaceKey);
			spaces.addChild(spaces.getChildCount(), node);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.manifoldcf.crawler.connectors.BaseRepositoryConnector#
	 * outputSpecificationBody
	 * (org.apache.manifoldcf.core.interfaces.IHTTPOutput, java.util.Locale,
	 * org.apache.manifoldcf.crawler.interfaces.DocumentSpecification,
	 * java.lang.String)
	 */
	@Override
	public void outputSpecificationBody(IHTTPOutput out, Locale locale,
			Specification ds, int connectionSequenceNumber,
			int actualSequenceNumber, String tabName)
			throws ManifoldCFException, IOException {

		// Output JIRAQuery tab
		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("TabName", tabName);
		paramMap.put("SeqNum", Integer.toString(connectionSequenceNumber));
		paramMap.put("SelectedNum", Integer.toString(actualSequenceNumber));

		fillInConfSpacesSpecificationMap(paramMap, ds);
		Messages.outputResourceWithVelocity(out, locale,
				EDIT_SPEC_FORWARD_SPACES, paramMap);
	}

	/*
	 * Header for the specification
	 * 
	 * @see org.apache.manifoldcf.crawler.connectors.BaseRepositoryConnector#
	 * outputSpecificationHeader
	 * (org.apache.manifoldcf.core.interfaces.IHTTPOutput, java.util.Locale,
	 * org.apache.manifoldcf.crawler.interfaces.DocumentSpecification,
	 * java.util.List)
	 */
	@Override
	public void outputSpecificationHeader(IHTTPOutput out, Locale locale,
			Specification ds, int connectionSequenceNumber,
			List<String> tabsArray) throws ManifoldCFException, IOException {

		tabsArray.add(Messages.getString(locale, CONF_SPACES_TAB_PROPERTY));

		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("SeqNum", Integer.toString(connectionSequenceNumber));

		Messages.outputResourceWithVelocity(out, locale,
				EDIT_SPEC_HEADER_FORWARD, paramMap);
	}

	/*
	 * Adding seed documents
	 * 
	 * @see org.apache.manifoldcf.crawler.connectors.BaseRepositoryConnector#
	 * addSeedDocuments
	 * (org.apache.manifoldcf.crawler.interfaces.ISeedingActivity,
	 * org.apache.manifoldcf.crawler.interfaces.DocumentSpecification, long,
	 * long, int)
	 */
	public String addSeedDocuments(ISeedingActivity activities,
			Specification spec, String lastSeedVersion, long seedTime,
			int jobMode) throws ManifoldCFException, ServiceInterruption {

		if (!isConnected()) {
			initConfluenceClient();
		}

		try {
			long lastStart = 0;
			long defaultSize = 50;

			/*
			 * Not uses delta seeding because Confluence can't be queried using
			 * dates or in a ordered way, only start and limit which can cause
			 * problems if an already indexed document is deleted, because we
			 * will miss some to-be indexed docs due to the last start parameter
			 * stored in the last execution
			 */
			// if(lastSeedVersion != null && !lastSeedVersion.isEmpty()) {
			// StringTokenizer tokenizer = new
			// StringTokenizer(lastSeedVersion,"|");
			//
			// lastStart = new Long(lastSeedVersion);
			// }

			if (Logging.connectors != null
					&& Logging.connectors.isDebugEnabled())
				Logging.connectors.debug(MessageFormat.format(
						"Starting from {0} and size {1}", new Object[] {
								lastStart, defaultSize }));

			Boolean isLast = true;

			Optional<String> space = getSpaceFromSpecification(spec);
			do {
				final ConfluenceResponse response = confluenceClient.getPages(
						(int) lastStart, (int) defaultSize, space);

				int count = 0;
				for (Page page : response.getResults()) {
					activities.addSeedDocument(page.getId());
					count++;
				}
				if (Logging.connectors != null
						&& Logging.connectors.isDebugEnabled())
					Logging.connectors.debug(MessageFormat.format(
							"Fetched and added {0} seed documents",
							new Object[] { new Integer(count) }));

				lastStart += count;
				isLast = response.isLast();
				if (Logging.connectors != null
						&& Logging.connectors.isDebugEnabled())
					Logging.connectors.debug(MessageFormat.format(
							"New start {0} and size {1}", new Object[] {
									lastStart, defaultSize }));
			} while (!isLast);

			if (Logging.connectors != null
					&& Logging.connectors.isDebugEnabled())
				Logging.connectors.debug(MessageFormat.format(
						"Recording {0} as last start id",
						new Object[] { lastStart }));
			return "";
		} catch (Exception e) {
			handleConfluenceDownException(e, "seeding");
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	private Optional<String> getSpaceFromSpecification(Specification spec) {
		String space = "";
		for (int i = 0, len = spec.getChildCount(); i < len; i++) {
			SpecificationNode sn = spec.getChild(i);
			if (sn.getType().equals(
					ConfluenceConfiguration.Specification.SPACES)) {
				for (int j = 0, sLen = sn.getChildCount(); j < sLen; j++) {
					SpecificationNode specNode = sn.getChild(j);
					if (specNode.getType().equals(
							ConfluenceConfiguration.Specification.SPACE)) {
						// spaceKeysList
						// .add(specNode
						// .getAttributeValue(ConfluenceConfiguration.Specification.SPACE_KEY_ATTRIBUTE));
						space = specNode
								.getAttributeValue(ConfluenceConfiguration.Specification.SPACE_KEY_ATTRIBUTE);
						break;
					}
				}

			}
		}
		return (Optional<String>) (space != null && !space.isEmpty() ? Optional
				.of(space) : Optional.absent());
	}

	protected static void handleConfluenceDownException(Exception e,
			String context) throws ManifoldCFException, ServiceInterruption {
		long currentTime = System.currentTimeMillis();

		// Server doesn't appear to by up. Try for a brief time then give up.
		String message = "Server appears down during " + context + ": "
				+ e.getMessage();
		Logging.connectors.warn(message, e);
		throw new ServiceInterruption(message, e, currentTime
				+ interruptionRetryTime, -1L, 3, true);
	}

	/*
	 * Process documents
	 * 
	 * @see org.apache.manifoldcf.crawler.connectors.BaseRepositoryConnector#
	 * processDocuments(java.lang.String[], java.lang.String[],
	 * org.apache.manifoldcf.crawler.interfaces.IProcessActivity,
	 * org.apache.manifoldcf.crawler.interfaces.DocumentSpecification,
	 * boolean[])
	 */
	@Override
	public void processDocuments(String[] documentIdentifiers,
			IExistingVersions statuses, Specification spec,
			IProcessActivity activities, int jobMode,
			boolean usesDefaultAuthority) throws ManifoldCFException,
			ServiceInterruption {

		Logging.connectors
				.debug("Process Confluence documents: Inside processDocuments");

		for (int i = 0; i < documentIdentifiers.length; i++) {
			String pageId = documentIdentifiers[i];
			String version = statuses.getIndexedVersionString(pageId);

			long startTime = System.currentTimeMillis();
			String errorCode = "FAILED";
			String errorDesc = StringUtils.EMPTY;
			Long fileSize = null;
			boolean doLog = true;

			try {
				if (Logging.connectors.isDebugEnabled()) {
					Logging.connectors
							.debug("Confluence: Processing document identifier '"
									+ pageId + "'");
				}

				/* Ensure Confluence client is connected */
				if (!isConnected()) {
					initConfluenceClient();
				}

				Page page = confluenceClient.getPage(pageId);
				
				/* Remove page if content is null */
				/* Content is null if there was an error trying to get the page */
				if (page.getContent() == null) {
					activities.deleteDocument(pageId);
					continue;
				}
				if (Logging.connectors.isDebugEnabled()) {
					Logging.connectors
							.debug("Confluence: This content exists: "
									+ page.getId());
				}
				RepositoryDocument rd = new RepositoryDocument();
				Date createdDate = page.getCreatedDate();
				Date lastModified = page.getLastModifiedDate();
				DateFormat df = DateFormat.getDateTimeInstance();
				
				/* Retain page in Manifold because it has not changed from last time
				 * This is needed to keep the identifier in Manifold data, because by default
				 * if a document is not retained nor ingested, it will be deleted by the framework
				 */
				if (version != null && version.equals(df.format(lastModified))) {
					activities.retainAllComponentDocument(pageId);
					continue;
				}
				
				/* Add repository document information */
				rd.setMimeType(page.getMimetype());
				if (createdDate != null)
					rd.setCreatedDate(createdDate);
				if (lastModified != null)
					rd.setModifiedDate(lastModified);
				rd.setIndexingDate(new Date());
				
				/* Adding Page Metadata */
				Map<String,String> pageMetadata = page.getMetadataAsMap();
				for(Entry<String,String> entry : pageMetadata.entrySet()) {
					rd.addField(entry.getKey(), entry.getValue());
				}
				
				String documentURI = page.getWebUrl();
				String content = page.getContent();
				
				/* Set repository document ACLs */
				rd.setSecurityACL(RepositoryDocument.SECURITY_TYPE_DOCUMENT,
						new String[] { page.getSpace() });
				rd.setSecurityDenyACL(RepositoryDocument.SECURITY_TYPE_DOCUMENT, new String[]{defaultAuthorityDenyToken});
				
				try {
					byte[] documentBytes = content
							.getBytes(StandardCharsets.UTF_8);
					InputStream is = new ByteArrayInputStream(documentBytes);
					try {
						rd.setBinary(is, documentBytes.length);
						rd.addField("size",
								String.valueOf(documentBytes.length));
						
						/* Ingest document */
						activities.ingestDocumentWithException(pageId,
								df.format(lastModified), documentURI, rd);
						/* No errors */
						errorCode = "OK";
						fileSize = new Long(documentBytes.length);
					} finally {
						is.close();
					}
				} catch (IOException e) {
					handleIOException(e);
				}

			} finally {
				if (doLog)
					activities.recordActivity(new Long(startTime),
							ACTIVITY_READ, fileSize, pageId, errorCode,
							errorDesc, null);
			}
		}
	}

	/**
	 * <p>
	 * Handles IO Exception to manage whether the exception is an interruption
	 * so that the process needs to be executed again later on
	 * </p>
	 * 
	 * @param e
	 *            The Exception
	 * @throws ManifoldCFException
	 * @throws ServiceInterruption
	 */
	private static void handleIOException(IOException e)
			throws ManifoldCFException, ServiceInterruption {
		if (!(e instanceof java.net.SocketTimeoutException)
				&& (e instanceof InterruptedIOException)) {
			throw new ManifoldCFException("Interrupted: " + e.getMessage(), e,
					ManifoldCFException.INTERRUPTED);
		}
		Logging.connectors.warn("IO exception: " + e.getMessage(), e);
		long currentTime = System.currentTimeMillis();
		throw new ServiceInterruption("IO exception: " + e.getMessage(), e,
				currentTime + 300000L, currentTime + 3 * 60 * 60000L, -1, false);
	}

}
