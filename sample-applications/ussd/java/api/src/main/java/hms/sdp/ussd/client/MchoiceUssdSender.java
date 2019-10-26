/*
 *   (C) Copyright 2010-2011 hSenid Software International (Pvt) Limited.
 *   All Rights Reserved.
 *
 *   These materials are unpublished, proprietary, confidential source code of
 *   hSenid Software International (Pvt) Limited and constitute a TRADE SECRET
 *   of hSenid Software International (Pvt) Limited.
 *
 *   hSenid Software International (Pvt) Limited retains all title to and intellectual
 *   property rights in these materials.
 *
 */
package hms.sdp.ussd.client;

import com.google.gson.Gson;
import hms.sdp.ussd.MchoiceUssdException;
import hms.sdp.ussd.MchoiceUssdMessage;
import hms.sdp.ussd.MchoiceUssdResponse;
import hms.sdp.ussd.impl.UssdAoRequestMessage;
import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.PostMethod;
import org.eclipse.jetty.http.HttpHeaders;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * $LastChangedDate$
 * $LastChangedBy$
 * $LastChangedRevision$
 */
public class MchoiceUssdSender {

    /**
     * Default USSD API version
     */
    private static final String DEFAULT_VERSION = "1.0";
    /**
     * USSD API version.
     */
    private String version;
    /**
     * ApplicationID for the application
     */
    private String appId;

    /**
     * Application password
     */
    private String password;

    /**
     * Server URI which to send the request.
     */
    private URI serverUri;

    private HttpClient httpClient;


    /**
     *
     * @param serverUri Server URL where the HTTP request will be sent when the sendMethod is called.
     * @param appId Application ID for your application.
     * @param password Password to your application.
     * @throws hms.sdp.ussd.MchoiceUssdException
     */
    public MchoiceUssdSender(String serverUri, String appId, String password) throws MchoiceUssdException {
        this(DEFAULT_VERSION, serverUri, appId, password);
    }

    /**
     *
     * @param version Version of the USSD API.
     * @param serverUri Server URL where the HTTP request will be sent when the sendMethod is called.
     * @param appId Application ID for your application.
     * @param password Password to your application.
     * @throws hms.sdp.ussd.MchoiceUssdException
     */
    public MchoiceUssdSender(String version, String serverUri, String appId, String password) throws MchoiceUssdException {
        this.version = version;
        this.appId = appId;
        this.password = password;
        try {
            this.serverUri = new URI(serverUri);
        } catch (URISyntaxException e) {
            throw new MchoiceUssdException(e);
        }
        httpClient = new HttpClient();
        addAuthentication(httpClient);
    }

    /**
     * Sends USSD message to the given address.
     * @param message message which has to be sent/
     * @param address MSISDN to send the message to/
     * @param conversationId Communication ID which the session was originated from.
     * @param characterEncoding Character Encoding of the requested message.
     * @param sessionTermination if the session Termination is true the session will be terminated after the message is sent.
     * @return Returns the status of the USSD message was requested.
     * @throws hms.sdp.ussd.MchoiceUssdException
     */
    public MchoiceUssdResponse sendMessage(String message, String address, String conversationId,
                                           String characterEncoding, boolean sessionTermination) throws MchoiceUssdException {

        final Gson gson = new Gson();
        final PostMethod postMethod = new PostMethod(serverUri.toString());
        postMethod.addRequestHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        postMethod.addRequestHeader(MchoiceUssdMessage.REQUEST_VERSION, version);
        postMethod.addRequestHeader(MchoiceUssdMessage.ENCODING, characterEncoding);
        postMethod.addRequestHeader(MchoiceUssdMessage.CONVERSATION, conversationId);
        final UssdAoRequestMessage ussdAoRequestMessage = new UssdAoRequestMessage();
        ussdAoRequestMessage.setMessage(message);
        ussdAoRequestMessage.setAddress(address);
        ussdAoRequestMessage.setSessionTermination(sessionTermination);
        postMethod.setRequestBody(gson.toJson(ussdAoRequestMessage));
        try {
            httpClient.executeMethod(postMethod);

            return gson.fromJson(postMethod.getResponseBodyAsString(), MchoiceUssdResponse.class);
        } catch (IOException e) {
            throw new MchoiceUssdException(e);
        }
    }

    /**
     * Sends USSD message to the given address.
     * @param message message which has to be sent/
     * @param address MSISDN to send the message to/
     * @param conversationId Communication ID which the session was originated from.
     * @param sessionTermination if the session Termination is true the session will be terminated after the message is sent.
     * @return Returns the status of the USSD message was requested.
     * @throws hms.sdp.ussd.MchoiceUssdException
     */
    public MchoiceUssdResponse sendMessage(String message, String address, String conversationId,
                                           boolean sessionTermination) throws MchoiceUssdException {
       return sendMessage(message, address, conversationId, "UTF-8", sessionTermination);
    }

    /**
     * Sets Authentication parameters to the POST request.
     * @param client Client which to set Authentication parameter
     */
    private void addAuthentication(HttpClient client) {
        //request for authentication.
        client.getParams().setAuthenticationPreemptive(true);
        //Create authentication parameters from the given appId and password (read about HTTP Basic Authentication
        // if you need more clarifications )
        Credentials credentials = new UsernamePasswordCredentials(appId, password);
        client.getState().setCredentials(new AuthScope(AuthScope.ANY), credentials);
    }

}
