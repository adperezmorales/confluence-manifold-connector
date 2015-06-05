package org.apache.manifoldcf.crawler.connectors.confluence.model;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ConfluenceResponse {

	private List<Page> results;
	private int start;
	private int limit;
	private Boolean isLast;
	
	public ConfluenceResponse(List<Page> results, int start, int limit, Boolean isLast) {
		this.results = results;
		this.start = start;
		this.limit = limit;
		this.isLast = isLast;
	}
	
	public List<Page> getResults() {
		return this.results;
	}
	
	public int getStart() {
		return this.start;
	}
	
	public int getLimit() {
		return this.limit;
	}
	
	public Boolean isLast() {
		return isLast;
	}
	
	public static ConfluenceResponse fromJson(JSONObject response) {
		List<Page> pages = new ArrayList<Page>();
		try {
			JSONArray jsonArray = response.getJSONArray("results");
			for(int i=0,size=jsonArray.length(); i<size;i++) {
				JSONObject jsonPage = jsonArray.getJSONObject(i);
				Page page = Page.fromJson(jsonPage);
				pages.add(page);
			}
			
			int limit = response.getInt("limit");
			int start = response.getInt("start");
			Boolean isLast = false;
			JSONObject links = response.getJSONObject("_links");
			if(links != null) {
				isLast = links.optString("next", "undefined").equalsIgnoreCase("undefined");
			}
			
			return new ConfluenceResponse(pages, start, limit, isLast);
			
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return new ConfluenceResponse(new ArrayList<Page>(), 0,0,false);
	}
}
