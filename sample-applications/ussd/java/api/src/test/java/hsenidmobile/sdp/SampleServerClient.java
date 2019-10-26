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
package hsenidmobile.sdp;

import com.google.gson.Gson;
import hms.sdp.ussd.MchoiceUssdMessage;
import hms.sdp.ussd.impl.UssdAtRequestMessage;
import hms.sdp.ussd.impl.UssdTerminateMessage;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.eclipse.jetty.http.HttpHeaders;
import org.junit.Test;

import java.io.IOException;

/**
 * $LastChangedDate$
 * $LastChangedBy$
 * $LastChangedRevision$
 */
public class SampleServerClient {

    @Test
    public void testUssdMessageClient() throws IOException {

        final Gson gson = new Gson();
        HttpClient httpClient = new HttpClient();
        final PostMethod postMethod = new PostMethod("http://127.0.0.1:8080/ussd/");
        postMethod.addRequestHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        postMethod.addRequestHeader(MchoiceUssdMessage.USSD_MESSAGE_TYPE, MchoiceUssdMessage.USSD_MESSAGE);
        postMethod.addRequestHeader("X-Requested-Shortcode", "4499");
        postMethod.addRequestHeader(MchoiceUssdMessage.CONVERSATION, "34234234");
        postMethod.addRequestHeader(MchoiceUssdMessage.REQUEST_VERSION, "1.0");
        final UssdAtRequestMessage ussdAtRequestMessage = new UssdAtRequestMessage();
        ussdAtRequestMessage.setMessage("Test Message");
        ussdAtRequestMessage.setAddress("12345678");
        ussdAtRequestMessage.setCorrelationId("89898989898");

        postMethod.setRequestBody(gson.toJson(ussdAtRequestMessage));
        httpClient.executeMethod(postMethod);
        System.out.println(postMethod.getResponseBodyAsString());
    }

    @Test
    public void testUssdTerminateClient() throws IOException {
        final Gson gson = new Gson();
        HttpClient httpClient = new HttpClient();
        final PostMethod postMethod = new PostMethod("http://127.0.0.1:8080/ussd/");
        postMethod.addRequestHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        postMethod.addRequestHeader(MchoiceUssdMessage.USSD_MESSAGE_TYPE, MchoiceUssdMessage.USSD_TERMINATE_MESSAGE);
        postMethod.addRequestHeader(MchoiceUssdMessage.REQUEST_VERSION, "1.0");
        postMethod.addRequestHeader(MchoiceUssdMessage.CONVERSATION, "34234234");
        final UssdTerminateMessage ussdMessage = new UssdTerminateMessage();
        ussdMessage.setCorrelationId("89898989898");
        ussdMessage.setAddress("947212345778");
        postMethod.setRequestBody(gson.toJson(ussdMessage));
        httpClient.executeMethod(postMethod);
        System.out.println(postMethod.getResponseBodyAsString());
    }

    @Test
    public void testUssdAliveClient() throws IOException {
        HttpClient httpClient = new HttpClient();
        final PostMethod postMethod = new PostMethod("http://127.0.0.1:8080/ussd/");
        postMethod.addRequestHeader(MchoiceUssdMessage.USSD_MESSAGE_TYPE, MchoiceUssdMessage.USSD_ALIVE_MESSAGE);
        postMethod.addRequestHeader(MchoiceUssdMessage.REQUEST_VERSION, "1.0");
        httpClient.executeMethod(postMethod);
        System.out.println(postMethod.getResponseBodyAsString());
    }



}
