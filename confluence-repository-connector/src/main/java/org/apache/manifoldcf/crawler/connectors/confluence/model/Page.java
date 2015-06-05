package org.apache.manifoldcf.crawler.connectors.confluence.model;

import java.util.Date;
import java.util.Map;

import org.apache.manifoldcf.core.common.DateParser;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.Maps;

/**
 * <p>
 * Page class
 * </p>
 * <p>
 * Represents a Confluence Page
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 */
public class Page {

	private static final String KEY_LINKS = "_links";
	private static final String KEY_ID = "id";
	private static final String KEY_SELF = "self";
	private static final String KEY_WEBUI = "webui";
	private static final String KEY_BASE = "base";
	private static final String KEY_KEY = "key";
	private static final String KEY_TITLE = "title";
	private static final String KEY_BODY = "body";
	private static final String KEY_VIEW = "view";
	private static final String KEY_VALUE = "value";
	private static final String KEY_SPACE = "space";
	private static final String KEY_HISTORY = "history";
	private static final String KEY_CREATED_DATE = "createdDate";
	private static final String KEY_CREATED_BY = "createdBy";
	private static final String KEY_BY = "by";
	private static final String KEY_TYPE = "type";
	private static final String KEY_DISPLAY_NAME = "displayName";
	private static final String KEY_USER_NAME = "username";
	private static final String KEY_VERSION = "version";
	private static final String KEY_WHEN = "when";
	
	private static final String PAGE_URL = "url";
	private static final String PAGE_WEBURL = "web_url";
	private static final String PAGE_LAST_MODIFIED = "lastModified";
	private static final String PAGE_MIMETYPE = "mimetype";
	private static final String PAGE_CREATOR = "creator";
	private static final String PAGE_CREATOR_USERNAME = "creatorUsername";
	private static final String PAGE_LAST_MODIFIER = "lastModifier";
	private static final String PAGE_LAST_MODIFIER_USERNAME = "lastModifierUsername";

	private String id;
	private String space;
	private String url;
	private String webUrl;
	private Date createdDate;
	private Date lastModified;
	private String type;
	private String title;
	private int version;
	private String creator;
	private String creatorUsername;
	private String lastModifier;
	private String lastModifierUsername;
	private String mimetype = "text/html";

	private String content;
	
	@SuppressWarnings("unused")
	private JSONObject delegated;
	
	public Page() {

	}

	public Page(Map<String, String> properties) {
	}

	public String getContent() {
		return this.content;
	}

	public String getId() {
		return this.id;
	}

	public String getType() {
		return this.type;
	}
	
	public String getMimetype() {
		return this.mimetype;
	}
	
	public int getVersion() {
		return this.version;
	}
	
	public String getTitle() {
		return this.title;
	}
	public String getWebUrl() {
		return this.webUrl;
	}

	public String getUrl() {
		return this.url;
	}

	public String getSpace() {
		return this.space;
	}

	public String getCreator() {
		return this.creator;
	}

	public String getCreatorUsername() {
		return this.creatorUsername;
	}

	public String getLastModifier() {
		return this.lastModifier;
	}

	public String getLastModifierUsername() {
		return this.lastModifierUsername;
	}

	public Date getCreatedDate() {
		return this.createdDate;
	}

	public Date getLastModifiedDate() {
		return this.lastModified;
	}

	public Map<String,String> getMetadataAsMap() {
		Map<String,String> pageMetadata = Maps.newHashMap();
		pageMetadata.put(KEY_ID, this.id);
		pageMetadata.put(KEY_TYPE, this.type);
		pageMetadata.put(KEY_TITLE, this.title);
		pageMetadata.put(KEY_SPACE, this.space);
		pageMetadata.put(PAGE_URL, this.url);
		pageMetadata.put(PAGE_WEBURL, this.webUrl);
		pageMetadata.put(KEY_CREATED_DATE, DateParser.formatISO8601Date(this.createdDate));
		pageMetadata.put(PAGE_LAST_MODIFIED, DateParser.formatISO8601Date(this.lastModified));
		pageMetadata.put(PAGE_MIMETYPE, this.mimetype);
		pageMetadata.put(KEY_VERSION, String.valueOf(this.version));
		pageMetadata.put(PAGE_CREATOR, this.creator);
		pageMetadata.put(PAGE_CREATOR_USERNAME, this.creatorUsername);
		pageMetadata.put(PAGE_LAST_MODIFIER, this.lastModifier);
		pageMetadata.put(PAGE_LAST_MODIFIER_USERNAME, this.lastModifierUsername);
		
		return pageMetadata;
	}
	
	
	public static Page fromJson(JSONObject page) {
		try {
			String id = page.getString(KEY_ID);
			String type = page.getString(KEY_TYPE);
			String title = page.getString(KEY_TITLE);

			Page p = new Page();
			p.delegated = page;

			/* Init Page fields */
			p.id = id;
			p.type = type;
			p.title = title;

			p.space = processSpace(page);

			/*
			 * Url & WebUrl
			 */
			JSONObject links = (JSONObject) page.get(KEY_LINKS);
			if (links != null) {
				p.url = links.optString(KEY_SELF, "");
				String webUrl = (String) links.optString(KEY_WEBUI, "");
				String base = (String) links.optString(KEY_BASE, "");
				p.webUrl = base + webUrl;

			}

			/*
			 * Created By and created Date
			 */
			JSONObject history = (JSONObject) page.optJSONObject(KEY_HISTORY);
			if (history != null) {

				p.createdDate = DateParser.parseISO8601Date(history.optString(
						KEY_CREATED_DATE, ""));
				JSONObject createdBy = (JSONObject) history.optJSONObject(KEY_CREATED_BY);
				if (createdBy != null) {
					p.creator = createdBy.optString(KEY_DISPLAY_NAME, "");
					p.creatorUsername = createdBy.optString(KEY_USER_NAME, "");
				}

			}

			/*
			 * Last modifier and Last modified date
			 */
			JSONObject version = (JSONObject) page.optJSONObject(KEY_VERSION);
			if (version != null) {
				JSONObject by = version.getJSONObject(KEY_BY);
				if (by != null) {
					p.lastModifier = by.optString(KEY_DISPLAY_NAME);
					p.lastModifierUsername = by.optString(KEY_USER_NAME, "");
				}

				p.lastModified = DateParser.parseISO8601Date(version.optString(
						KEY_WHEN, ""));
			}

			/*
			 * Page Content
			 */
			JSONObject body = (JSONObject) page.optJSONObject(KEY_BODY);
			if (body != null) {
				JSONObject view = (JSONObject) body.optJSONObject(KEY_VIEW);
				if (view != null) {
					p.content = view.optString(KEY_VALUE, null);
				}
			}

			return p;

		} catch (JSONException e) {
			e.printStackTrace();
		}

		return new Page();

	}

	private static String processSpace(JSONObject page) {
		/* Page */
		try {
			JSONObject space = (JSONObject) page.get(KEY_SPACE);
			if (space != null)
			return space.optString(KEY_KEY, "");
		}
		catch(JSONException e) {
			return "";
		}
		return "";
	}
}
