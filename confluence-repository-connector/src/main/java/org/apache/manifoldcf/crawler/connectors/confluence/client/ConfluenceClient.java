package org.apache.manifoldcf.crawler.connectors.confluence.client;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.manifoldcf.crawler.connectors.confluence.model.ConfluenceResponse;
import org.apache.manifoldcf.crawler.connectors.confluence.model.Page;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

public class ConfluenceClient {

	private Logger logger = LoggerFactory.getLogger(ConfluenceClient.class);

	private String protocol;
	private Integer port;
	private String host;
	private String path;
	private String username;
	private String password;
	
	private CloseableHttpClient httpClient;

	public ConfluenceClient(String protocol, String host, Integer port,
			String path, String username, String password)
			 {
		this.protocol = protocol;
		this.host = host;
		this.port = port;
		this.path = path;
		this.username = username;
		this.password = password;
		
		connect();
	}

	private void connect() {
		httpClient = HttpClients.createDefault();
	}
	
	public void close() {
		if(httpClient != null) {
			try {
				httpClient.close();
			} catch (IOException e) {
				logger.debug("Error closing http connection. Reason: {}", e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	public boolean check() throws Exception {
		HttpResponse response;
		try {
			if(httpClient == null) {
				connect();
			}
			
			String url = String.format("%s://%s:%s/%s?limit=1", protocol, host,
					port, path);
			logger.debug(
					"[Processing] Hitting url: {} for confluence status check fetching : ",
					"Confluence URL");
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

	private HttpGet createGetRequest(String url) {
		HttpGet httpGet = new HttpGet(url);
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

	public ConfluenceResponse getPages() throws Exception {
		return getPages(0, 50, Optional.<String>absent());
	}

	/**
	 * 
	 * @param start
	 * @param limit
	 * @param space
	 * @return
	 * @throws Exception
	 */
	public ConfluenceResponse getPages(int start, int limit, Optional<String> space) throws Exception {
		String url = String.format("%s://%s:%s/%s?limit=%s&start=%s", protocol, host,
				port, path, limit, start);
		if(space.isPresent()) {
			url = String.format("%s&spaceKey=%s", url, space.get());
		}
		return getRealPages(url);
	}

	private ConfluenceResponse getRealPages(String url) throws Exception {
		CloseableHttpClient httpClient = HttpClients.createDefault();
		logger.debug("[Processing] Hitting url for document actions: {}", url);

		try {
			HttpGet httpGet = createGetRequest(url);
			HttpResponse response = httpClient.execute(httpGet);
			HttpEntity entity = response.getEntity();
			ConfluenceResponse confluenceResponse = responseFromHttpEntity(entity);
			EntityUtils.consume(entity);
			return confluenceResponse;
		} catch (IOException e) {
			logger.error("[Processing] Failed to get page(s)", e);
			throw new Exception("Confluence appears to be down", e);
		}
	}

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
			// TODO Auto-generated catch block
			logger.debug("Error parsing JSON response");
			throw new Exception();
		}

	}

	public Page getPage(String pageId) {
		CloseableHttpClient httpClient = HttpClients.createDefault();
		String url = String.format("%s://%s:%s%s%s?expand=body.view,metadata.labels,space,history,version", protocol, host,
				port, path, pageId);
		
		logger.debug("[Processing] Hitting url for getting document content : {}", url);

		try {
			HttpGet httpGet = createGetRequest(url);
			HttpResponse response = httpClient.execute(httpGet);
			if(response.getStatusLine().getStatusCode() != 200) {
				throw new Exception("Confluence error. "+response.getStatusLine().getStatusCode()+" "+response.getStatusLine().getReasonPhrase());
			}
			HttpEntity entity = response.getEntity();
			Page page = pageFromHttpEntity(entity);
			EntityUtils.consume(entity);
			return page;
		} catch (Exception e) {
			logger.error("[Processing] Failed to get page {0}. Error: {1}", url, e.getMessage());
		}
		
		return new Page(null);
	}
	
	private Page pageFromHttpEntity(HttpEntity entity)
			throws Exception {
		String stringEntity = EntityUtils.toString(entity);

		JSONObject responseObject;
		try {
			responseObject = new JSONObject(stringEntity);
			Page response = Page
					.fromJson(responseObject);
			
			return response;
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			logger.debug("Error parsing JSON page response data");
			throw new Exception("Error parsing JSON page respnse data");
		}

	}
	
	
	private boolean useBasicAuthentication() {
		return this.username != null && !"".equals(username)
				&& this.password != null;
	}
}
