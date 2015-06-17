package org.apache.manifoldcf.crawler.connectors.confluence.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.manifoldcf.crawler.connectors.confluence.model.Attachment;
import org.apache.manifoldcf.crawler.connectors.confluence.model.ConfluenceResponse;
import org.apache.manifoldcf.crawler.connectors.confluence.model.MutableAttachment;
import org.apache.manifoldcf.crawler.connectors.confluence.model.Page;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * <p>
 * ConfluenceClient class
 * </p>
 * <p>
 * This class is intended to be used to interact with Confluence REST API
 * </p>
 * <p>
 * There are some methods that make use of the Confluence JSON-RPC 2.0 API, but
 * until all the methods are ported to the new REST API, we will have to use
 * them to leverage all the features provided by Confluence
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 *
 */
public class ConfluenceClient {

	private static final String CONTENT_PATH = "/rest/api/content";
	private static final String EXPANDABLE_PARAMETERS = "expand=body.view,metadata.labels,space,history,version";
	private static final String CHILD_ATTACHMENTS_PATH = "/child/attachment/";

	private Logger logger = LoggerFactory.getLogger(ConfluenceClient.class);

	private String protocol;
	private Integer port;
	private String host;
	private String path;
	private String username;
	private String password;

	private CloseableHttpClient httpClient;

	/**
	 * <p>Creates a new client instance using the given parameters</p>
	 * @param protocol the protocol
	 * @param host the host
	 * @param port the port
	 * @param path the path to Confluence instance
	 * @param username the username used to make the requests. Null or empty to use anonymous user
	 * @param password the password
	 */
	public ConfluenceClient(String protocol, String host, Integer port,
			String path, String username, String password) {
		this.protocol = protocol;
		this.host = host;
		this.port = port;
		this.path = path;
		this.username = username;
		this.password = password;

		connect();
	}

	/**
	 * <p>Connect methods used to initialize the underlying client</p>
	 */
	private void connect() {
		httpClient = HttpClients.createDefault();
	}

	/**
	 * <p>Close the client. No further requests can be done</p>
	 */
	public void close() {
		if (httpClient != null) {
			try {
				httpClient.close();
			} catch (IOException e) {
				logger.debug("Error closing http connection. Reason: {}",
						e.getMessage());
				e.printStackTrace();
			}
		}
	}

	/**
	 * <p>Check method used to test if Confluence instance is up and running</p>
	 * 
	 * @return a {@code Boolean} indicating whether the Confluence instance is alive or not
	 * 
	 * @throws Exception
	 */
	public boolean check() throws Exception {
		HttpResponse response;
		try {
			if (httpClient == null) {
				connect();
			}

			String url = String.format("%s://%s:%s/%s/%s?limit=1", protocol, host,
					port, path, CONTENT_PATH);
			logger.debug(
					"[Processing] Hitting url: {} for confluence status check fetching : ",
					"Confluence URL", sanitizeUrl(url));
			HttpGet httpGet = createGetRequest(url);
			response = httpClient.execute(httpGet);
			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode != 200)
				throw new Exception(
						"[Checking connection] Confluence server appears to be down");
			else
				return true;
		} catch (IOException e) {
			logger.warn(
					"[Checking connection] Confluence server appears to be down",
					e);
			throw new Exception("Confluence appears to be down", e);
		}
	}

	/**
	 * <p>
	 * Create a get request for the given url
	 * </p>
	 * 
	 * @param url
	 *            the url
	 * @return the created {@code HttpGet} instance
	 */
	private HttpGet createGetRequest(String url) {
		String sanitizedUrl = sanitizeUrl(url);
		HttpGet httpGet = new HttpGet(sanitizedUrl);
		httpGet.addHeader("Accept", "application/json");
		if (useBasicAuthentication()) {
			httpGet.addHeader(
					"Authorization",
					"Basic "
							+ Base64.encodeBase64String(String.format("%s:%s",
									this.username, this.password).getBytes(
									Charset.forName("UTF-8"))));
		}
		return httpGet;
	}

	/**
	 * <p>
	 * Get a list of Confluence pages
	 * </p>
	 * 
	 * @return a {@code ConfluenceResponse} containing the result pages and
	 *         some pagination values
	 * @throws Exception
	 */
	public ConfluenceResponse getPages() throws Exception {
		return getPages(0, 50, Optional.<String> absent());
	}

	/**
	 * <p>
	 * Get a list of Confluence pages using pagination
	 * </p>
	 * 
	 * @param start The start value to get pages from
	 * @param limit The number of pages to get from start
	 * @return a {@code ConfluenceResponse} containing the result pages and
	 *         some pagination values
	 * @throws Exception
	 */
	public ConfluenceResponse getPages(int start, int limit,
			Optional<String> space) throws Exception {
		String url = String.format("%s://%s:%s/%s/%s?limit=%s&start=%s", protocol,
				host, port, path, CONTENT_PATH, limit, start);
		if (space.isPresent()) {
			url = String.format("%s&spaceKey=%s", url, space.get());
		}
		return getRealPages(url);
	}

	/**
	 * <p>Get the pages from the given url</p>
	 * @param url The url identifying the REST resource to get the pages
	 * @return a {@code ConfluenceResponse} containing the page results
	 * @throws Exception
	 */
	private ConfluenceResponse getRealPages(String url) throws Exception {
		logger.debug("[Processing] Hitting url for document actions: {}", sanitizeUrl(url));

		try {
			HttpGet httpGet = createGetRequest(url);
			HttpResponse response = executeRequest(httpGet);
			ConfluenceResponse confluenceResponse = responseFromHttpEntity(response
					.getEntity());
			EntityUtils.consume(response.getEntity());
			return confluenceResponse;
		} catch (IOException e) {
			logger.error("[Processing] Failed to get page(s)", e);
			throw new Exception("Confluence appears to be down", e);
		}
	}

	/**
	 * <p>Creates a ConfluenceResponse from the entity returned in the HttpResponse</p>
	 * @param entity the {@code HttpEntity} to extract the response from
	 * @return a {@code ConfluenceResponse} with the requested information
	 * @throws Exception
	 */
	private ConfluenceResponse responseFromHttpEntity(HttpEntity entity)
			throws Exception {
		String stringEntity = EntityUtils.toString(entity);

		JSONObject responseObject;
		try {
			responseObject = new JSONObject(stringEntity);
			ConfluenceResponse response = ConfluenceResponse
					.fromJson(responseObject);
			if (response.getResults().size() == 0) {
				logger.warn("[Processing] No pages found in the Confluence response");
			}

			return response;
		} catch (JSONException e) {
			logger.debug("Error parsing JSON response");
			throw new Exception();
		}

	}
	
	/**
	 * <p>Get the attachments of the given page</p>
	 * @param pageId the page id
	 * @return a {@code ConfluenceResponse} instance containing the attachment results and some pagination values</p>
	 * @throws Exception
	 */
	public ConfluenceResponse getPageAttachments(String pageId)
			throws Exception {
		return getPageAttachments(pageId, 0, 50);
	}

	/**
	 * <p>Get the attachments of the given page using pagination</p>
	 * @param pageId the page id
	 * @param start The start value to get attachments from
	 * @param limit The number of attachments to get from start
	 * @return a {@code ConfluenceResponse} instance containing the attachment results and some pagination values</p>
	 * @throws Exception
	 */
	public ConfluenceResponse getPageAttachments(String pageId, int start,
			int limit) throws Exception {
		String url = String.format("%s://%s:%s/%s/%s/%s%s?limit=%s&start=%s",
				protocol, host, port, path, CONTENT_PATH, pageId, CHILD_ATTACHMENTS_PATH,
				limit, start);
		return getRealPages(url);
	}
	
	/**
	 * <p>
	 * Gets a specific attachment contained in the specific page
	 * </p>
	 * 
	 * @param attachmentId
	 * @param pageId
	 * @return the {@code Attachment} instance
	 */
	public Attachment getAttachment(String attachmentId) {
		String url = String
				.format("%s://%s:%s/%s/%s/%s?%s",
						protocol, host, port, path, CONTENT_PATH, attachmentId, EXPANDABLE_PARAMETERS);
		logger.debug(
				"[Processing] Hitting url for getting document content : {}",
				sanitizeUrl(url));
		try {
			HttpGet httpGet = createGetRequest(url);
			HttpResponse response = executeRequest(httpGet);
			HttpEntity entity = response.getEntity();
			MutableAttachment attachment = attachmentFromHttpEntity(entity);
			EntityUtils.consume(entity);
			retrieveAndSetAttachmentContent(attachment);
			return attachment;
		} catch (Exception e) {
			logger.error("[Processing] Failed to get attachment {}. Error: {}",
					url, e.getMessage());
		}

		return new Attachment();
	}

	/**
	 * <p>
	 * Downloads and retrieves the attachment content, setting it in the given
	 * {@code Attachment} instance
	 * </p>
	 * 
	 * @param attachment
	 *            the {@code Attachment} instance to download and set the
	 *            content
	 * @throws Exception
	 */
	private void retrieveAndSetAttachmentContent(MutableAttachment attachment)
			throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(attachment.getBaseUrl()).append(attachment.getUrlContext())
				.append(attachment.getDownloadUrl());
		String url = sanitizeUrl(sb.toString());
		logger.debug(
				"[Processing] Hitting url for getting attachment content : {}",
				url);
		try {
			HttpGet httpGet = createGetRequest(url);
			HttpResponse response = executeRequest(httpGet);
			attachment.setLength(response.getEntity().getContentLength());
			byte[] byteContent = IOUtils.toByteArray(response.getEntity()
					.getContent());
			EntityUtils.consumeQuietly(response.getEntity());
			attachment.setContentStream(new ByteArrayInputStream(byteContent));
		} catch (Exception e) {

			logger.error(
					"[Processing] Failed to get attachment content from {}. Error: {}",
					url, e.getMessage());
			throw e;
		}

	}


	/**
	 * <p>Get a Confluence page identified by its id</p>
	 * @param pageId the page id
	 * @return the Confluence page
	 */
	public Page getPage(String pageId) {
		String url = String
				.format("%s://%s:%s/%s/%s/%s?%s",
						protocol, host, port, path, CONTENT_PATH, pageId, EXPANDABLE_PARAMETERS);
		url = sanitizeUrl(url);
		logger.debug(
				"[Processing] Hitting url for getting document content : {}",
				url);
		try {
			HttpGet httpGet = createGetRequest(url);
			HttpResponse response = executeRequest(httpGet);
			HttpEntity entity = response.getEntity();
			Page page = pageFromHttpEntity(entity);
			EntityUtils.consume(entity);
			return page;
		} catch (Exception e) {
			logger.error("[Processing] Failed to get page {0}. Error: {1}",
					url, e.getMessage());
		}

		return new Page();
	}

	/**
	 * <p>Execute the given {@code HttpUriRequest} using the configured client</p> 
	 * @param request the {@code HttpUriRequest} to be executed
	 * @return the {@code HttpResponse} object returned from the server
	 * @throws Exception
	 */
	private HttpResponse executeRequest(HttpUriRequest request)
			throws Exception {
		CloseableHttpClient httpClient = HttpClients.createDefault();
		String url = request.getURI().toString();
		logger.debug(
				"[Processing] Hitting url for getting document content : {}",
				url);

		try {
			HttpResponse response = httpClient.execute(request);
			if (response.getStatusLine().getStatusCode() != 200) {
				throw new Exception("Confluence error. "
						+ response.getStatusLine().getStatusCode() + " "
						+ response.getStatusLine().getReasonPhrase());
			}
			return response;
		} catch (Exception e) {
			logger.error("[Processing] Failed to get page {}. Error: {}",
					url, e.getMessage());
			throw e;
		}
	}

	/**
	 * <p>Creates a Confluence page object from the given entity returned by the server</p>
	 * @param entity the {@code HttpEntity} to create the {@code Page} from
	 * @return the Confluence page instance
	 * @throws Exception
	 */
	private Page pageFromHttpEntity(HttpEntity entity) throws Exception {
		String stringEntity = EntityUtils.toString(entity);

		JSONObject responseObject;
		try {
			responseObject = new JSONObject(stringEntity);
			Page response = Page.builder().fromJson(responseObject);

			return response;
		} catch (JSONException e) {
			logger.debug("Error parsing JSON page response data");
			throw new Exception("Error parsing JSON page response data");
		}
	}

	/**
	 * <p>Creates a {@code MutableAttachment} object from the given entity returned by the server</p>
	 * @param entity the {@code HttpEntity} to create the {@code MutableAttachment} from
	 * @return the Confluence MutableAttachment instance
	 * @throws Exception
	 */
	private MutableAttachment attachmentFromHttpEntity(HttpEntity entity)
			throws Exception {
		String stringEntity = EntityUtils.toString(entity);
		JSONObject responseObject;
		try {
			responseObject = new JSONObject(stringEntity);
			MutableAttachment response = (MutableAttachment) Attachment
					.builder()
					.fromJson(responseObject, new MutableAttachment());
			return response;
		} catch (JSONException e) {
			logger.debug("Error parsing JSON page response data");
			throw new Exception("Error parsing JSON page response data");
		}
	}

	/**
	 * <p>Method to check if basic authentication must be used</p>
	 * @return {@code Boolean} indicating whether basic authentication must be used or not
	 */
	private boolean useBasicAuthentication() {
		return this.username != null && !"".equals(username)
				&& this.password != null;
	}

	/**
	 * <p>
	 * Sanitize the given url replacing the appearance of more than one slash by
	 * only one slash
	 * </p>
	 * 
	 * @param url
	 *            The url to sanitize
	 * @return the sanitized url
	 */
	private String sanitizeUrl(String url) {
		int colonIndex = url.indexOf(":");
		String urlWithoutProtocol = url.startsWith("http") ? url.substring(colonIndex+3) : url;
		String sanitizedUrl = urlWithoutProtocol.replaceAll("\\/+", "/");
		return url.substring(0,colonIndex) + "://" + sanitizedUrl;
	}
}
