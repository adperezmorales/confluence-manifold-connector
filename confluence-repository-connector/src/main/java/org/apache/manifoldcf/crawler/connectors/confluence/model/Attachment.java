package org.apache.manifoldcf.crawler.connectors.confluence.model;

import java.io.InputStream;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * <p>
 * Attachment class
 * </p>
 * <p>
 * Represents a Confluence Attachment
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 */
public class Attachment extends Page {

	protected static final String KEY_DOWNLOAD = "download";
	protected static final String KEY_EXTENSIONS = "extensions";
	protected String downloadUrl;
	protected InputStream contentStream;

	public static AttachmentBuilder builder() {
		return new AttachmentBuilder();
	}

	public String getDownloadUrl() {
		return this.downloadUrl;
	}

	@Override
	public boolean hasContent() {
		return (this.length > 0 && this.hasContentStream()) || (this.downloadUrl != null && !this.downloadUrl.isEmpty());
	}

	public Boolean hasContentStream() {
		return this.contentStream != null;
	}

	@Override
	public InputStream getContentStream() {
		if(hasContentStream()) {
			return this.contentStream;
		}
		return super.getContentStream();
	}

	@Override
	protected void refineMetadata(Map<String, String> metadata) {
		super.refineMetadata(metadata);
		metadata.put("downloadUrl", this.getBaseUrl() + this.getUrlContext()
				+ downloadUrl);
	}

	/**
	 * <p>
	 * AttachmentBuilder internal class
	 * </p>
	 * <p>
	 * Used to build Attachments
	 * </p>
	 * 
	 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
	 *
	 */
	public static class AttachmentBuilder extends Page.PageBuilder {
		
		@Override
		public Attachment fromJson(JSONObject jsonPage) {
			return fromJson(jsonPage, new Attachment());
		}

		public Attachment fromJson(JSONObject jsonPage, Attachment attachment) {
			super.fromJson(jsonPage, attachment);

			try {
				/*
				 * Download URL
				 */

				JSONObject links = (JSONObject) jsonPage.get(Page.KEY_LINKS);
				if (links != null) {
					attachment.downloadUrl = links.optString(KEY_DOWNLOAD, "");
				}

				/*
				 * Extensions
				 */
				JSONObject extensions = (JSONObject) jsonPage
						.get(KEY_EXTENSIONS);
				if (extensions != null) {
					attachment.mediaType = extensions.optString(
							Page.KEY_MEDIATYPE, "");
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}

			return attachment;
		}

	}
}