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
import hms.sdp.ussd.MchoiceUssdMessage;
import hms.sdp.ussd.MchoiceUssdTerminateMessage;
import hms.sdp.ussd.impl.UssdAtRequestMessage;
import hms.sdp.ussd.impl.UssdTerminateMessage;
import org.apache.log4j.Logger;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * $LastChangedDate$
 * $LastChangedBy$
 * $LastChangedRevision$
 */
public abstract class MchoiceUssdReceiver extends HttpServlet {

    private static final Logger logger = Logger.getLogger(MchoiceUssdReceiver.class);

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        final String ussdMessageType = req.getHeader(MchoiceUssdMessage.USSD_MESSAGE_TYPE);
        final Gson gson = new Gson();
        final ServletInputStream inputStream = req.getInputStream();
        if (MchoiceUssdMessage.USSD_MESSAGE.equals(ussdMessageType)) {
            final UssdAtRequestMessage atRequestMessage = gson.fromJson(readBody(inputStream), UssdAtRequestMessage.class);
            atRequestMessage.setShortcode(req.getHeader("X-Requested-Shortcode"));
            atRequestMessage.setVersion(req.getHeader(MchoiceUssdMessage.REQUEST_VERSION));
            atRequestMessage.setConversationId(req.getHeader(MchoiceUssdMessage.CONVERSATION));
            onMessage(atRequestMessage);
        } else if (MchoiceUssdMessage.USSD_TERMINATE_MESSAGE.equals(ussdMessageType)) {
            final UssdTerminateMessage message = gson.fromJson(readBody(inputStream), UssdTerminateMessage.class);
            message.setVersion(req.getHeader(MchoiceUssdMessage.REQUEST_VERSION));
            message.setConversationId(req.getHeader(MchoiceUssdMessage.CONVERSATION));
            onSessionTerminate(message);
        } else if (MchoiceUssdMessage.USSD_ALIVE_MESSAGE.equals(ussdMessageType)) {
            resp.setStatus(HttpServletResponse.SC_ACCEPTED);
        } else {
            logger.debug("Message type not identified");
        }
    }

    private String readBody(InputStream inputStream) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));

        StringBuilder line = new StringBuilder();
        String li;
        while ((li = in.readLine()) != null) {
            line.append(li);
        }
        return line.toString();
    }

    public abstract void onMessage(MchoiceUssdMessage atRequestMessage);

    public abstract void onSessionTerminate(MchoiceUssdTerminateMessage message);
}
