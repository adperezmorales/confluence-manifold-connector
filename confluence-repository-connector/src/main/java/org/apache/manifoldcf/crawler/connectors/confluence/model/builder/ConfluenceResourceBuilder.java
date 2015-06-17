package org.apache.manifoldcf.crawler.connectors.confluence.model.builder;

import org.apache.manifoldcf.crawler.connectors.confluence.model.ConfluenceResource;
import org.json.JSONObject;

/**
 * <p>ConfluenceResourceBuilder interface</p>
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 *
 * @param <T> Subtype of ConfluenceDocument to be built
 */
public interface ConfluenceResourceBuilder<T extends ConfluenceResource> {

	/**
	 * <p>Creates a <T> instance from a JSON representation 
	 * @param jsonDocument
	 * @return T instance
	 */
	T fromJson(JSONObject jsonDocument);
	
	/**
	 * <p>Populates the given <T> instance from a JSON representation and return it</p>
	 * @param jsonDocument
	 * @return T instance
	 */
	T fromJson(JSONObject jsonDocument, T document);
}
