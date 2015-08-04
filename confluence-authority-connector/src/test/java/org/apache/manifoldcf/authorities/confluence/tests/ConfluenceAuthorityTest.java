package org.apache.manifoldcf.authorities.confluence.tests;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.manifoldcf.authorities.authorities.confluence.ConfluenceAuthorityConnector;
import org.apache.manifoldcf.authorities.authorities.confluence.client.ConfluenceClient;
import org.apache.manifoldcf.authorities.authorities.confluence.model.ConfluenceUser;
import org.apache.manifoldcf.authorities.interfaces.AuthorizationResponse;
import org.apache.manifoldcf.authorities.interfaces.IAuthorityConnector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConfluenceAuthorityTest {

	@Mock
	private ConfluenceClient client;
	
	private ConfluenceAuthorityConnector connector;
	
	@Before
	public void setup() throws Exception{
		connector = new ConfluenceAuthorityConnector();
		connector.setConfluenceClient(client);
	}
	
	@Test
	public void checkMockInjection() throws Exception{
		when(client.check()).thenReturn(true);
		Assert.assertEquals(connector.check(), "Connection working");
	}
	
	@Test
	public void checkUserNotFound() throws Exception{
		ConfluenceUser user = mock(ConfluenceUser.class);
		when(user.getUsername()).thenReturn(null);
		when(client.getUserAuthorities(anyString())).thenReturn(user);
		AuthorizationResponse response = connector.getAuthorizationResponse(anyString());
		String[] tokens = response.getAccessTokens();
		Assert.assertEquals(tokens.length, 1);
		Assert.assertEquals(tokens[0], IAuthorityConnector.GLOBAL_DENY_TOKEN);
		Assert.assertEquals(response.getResponseStatus(), AuthorizationResponse.RESPONSE_USERNOTFOUND);
	}
	
	@Test
	public void checkUserFound() throws Exception{
		ConfluenceUser user = mock(ConfluenceUser.class);
		when(user.getUsername()).thenReturn("A");
		List<String> tokens = new ArrayList<String>();
		tokens.add("B");
		when(user.getAuthorities()).thenReturn(tokens);
		when(client.getUserAuthorities(anyString())).thenReturn(user);
		AuthorizationResponse response = connector.getAuthorizationResponse(anyString());
		String[] tokens_aux = response.getAccessTokens();
		Assert.assertEquals(tokens_aux.length, 1);
		Assert.assertEquals(tokens_aux[0], tokens.get(0));
		Assert.assertEquals(response.getResponseStatus(), AuthorizationResponse.RESPONSE_OK);
	}
	
}
