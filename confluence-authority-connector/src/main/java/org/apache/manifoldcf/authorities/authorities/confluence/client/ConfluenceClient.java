package org.apache.manifoldcf.authorities.authorities.confluence.client;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.manifoldcf.authorities.authorities.confluence.exception.ConfluenceException;
import org.apache.manifoldcf.authorities.authorities.confluence.model.ConfluenceUser;
import org.apache.manifoldcf.authorities.authorities.confluence.model.Space;
import org.apache.manifoldcf.authorities.authorities.confluence.model.Spaces;
import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class ConfluenceClient {

	private static final String VIEW_PERMISSION = "view";
	
	private Logger logger = LoggerFactory.getLogger(ConfluenceClient.class);

	private String protocol;
	private Integer port;
	private String host;
	private String path;
	private String username;
	private String password;

	private CloseableHttpClient httpClient;

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

	private void connect() {
		httpClient = HttpClients.createDefault();
	}

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

	public boolean check() throws Exception {
		try {
			if (httpClient == null) {
				connect();
			}
			getSpaces();
			return true;
		} catch (Exception e) {
			logger.warn(
					"[Checking connection] Confluence server appears to be down",
					e);
			throw e;
		}
	}

	public ConfluenceUser getUserAuthorities(String username) throws Exception {
		List<String> authorities = Lists.<String>newArrayList();
		Spaces spaces = getSpaces();
		for(Space space: spaces) {
			List<String> permissions = getSpacePermissionsForUser(space, username);
			if(permissions.contains(VIEW_PERMISSION)) {
				authorities.add(space.getKey());
			}
		}
		
		return new ConfluenceUser(username, authorities);
	
	}
	private HttpPost createPostRequest(String url) {
		HttpPost httpPost = new HttpPost(url);
		httpPost.addHeader("Accept", "application/json");
		httpPost.addHeader("Content-Type", "application/json");
		if (useBasicAuthentication()) {
			httpPost.addHeader(
					"Authorization",
					"Basic "
							+ Base64.encodeBase64String(String.format("%s:%s",
									this.username, this.password).getBytes(
									Charset.forName("UTF-8"))));
		}
		return httpPost;
	}

	private Spaces getSpaces() throws Exception {
		String url = String.format("%s://%s:%s%sgetSpaces", protocol, host,
				port, path);

		logger.debug(
				"[Processing] Hitting url for getting Confluence spaces : {}",
				url);

		HttpPost httpPost = createPostRequest(url);
		httpPost.setEntity(new StringEntity("[]"));
		HttpResponse response = httpClient.execute(httpPost);
		if (response.getStatusLine().getStatusCode() != 200) {
			throw new ConfluenceException("Confluence error. "
					+ response.getStatusLine().getStatusCode() + " "
					+ response.getStatusLine().getReasonPhrase());
		}
		HttpEntity entity = response.getEntity();
		Spaces spaces = spacesFromHttpEntity(entity);
		EntityUtils.consume(entity);
		return spaces;
	}
	
	private List<String> getSpacePermissionsForUser(Space space, String username) throws Exception {
		String url = String.format("%s://%s:%s%sgetPermissionsForUser", protocol, host,
				port, path);

		logger.debug(
				"[Processing] Hitting url {} for getting Confluence permissions for user {} in space {}",
				url, username, space.getKey());

		HttpPost httpPost = createPostRequest(url);
		JSONArray jsonArray = new JSONArray();
		jsonArray.put(space.getKey());
		jsonArray.put(username);
		StringEntity stringEntity = new StringEntity(jsonArray.toString());
		httpPost.setEntity(stringEntity);
		HttpResponse response = httpClient.execute(httpPost);
		if (response.getStatusLine().getStatusCode() != 200) {
			throw new ConfluenceException("Confluence error. "
					+ response.getStatusLine().getStatusCode() + " "
					+ response.getStatusLine().getReasonPhrase());
		}
		HttpEntity entity = response.getEntity();
		List<String> permissions = permissionsFromHttpEntity(entity);
		EntityUtils.consume(entity);
		return permissions;
	}

	private Spaces spacesFromHttpEntity(HttpEntity entity) throws Exception {
		String stringEntity = EntityUtils.toString(entity);

		JSONArray responseObject;
		try {
			responseObject = new JSONArray(stringEntity);
			Spaces response = Spaces.fromJson(responseObject);

			return response;
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			logger.debug("Error parsing JSON spaces response data");
			throw new Exception("Error parsing JSON spaces response data");
		}

	}
	
	private List<String> permissionsFromHttpEntity(HttpEntity entity) throws Exception {
		String stringEntity = EntityUtils.toString(entity);

		JSONArray responseObject;
		List<String> permissions = Lists.newArrayList();
		try {
			responseObject = new JSONArray(stringEntity);
			for(int i=0,len=responseObject.length();i<len;i++) {
				permissions.add(responseObject.getString(i));
			}

			return permissions;
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			logger.debug("Error parsing JSON space permissions response data");
			throw new Exception("Error parsing JSON space permissions respnse data");
		}

	}

	private boolean useBasicAuthentication() {
		return this.username != null && !"".equals(username)
				&& this.password != null;
	}
}
