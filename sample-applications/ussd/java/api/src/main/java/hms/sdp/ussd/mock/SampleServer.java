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
package hms.sdp.ussd.mock;

import com.google.gson.Gson;
import hms.sdp.ussd.MchoiceUssdMessage;
import hms.sdp.ussd.MchoiceUssdResponse;
import hms.sdp.ussd.impl.UssdAoRequestMessage;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

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
public class SampleServer extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Gson gson = new Gson();
        final ServletInputStream inputStream = req.getInputStream();
        final UssdAoRequestMessage ussdAoRequestMessage = gson.fromJson(readBody(inputStream), UssdAoRequestMessage.class);
        ussdAoRequestMessage.setConversationId(req.getHeader(MchoiceUssdMessage.CONVERSATION));
        System.out.println(ussdAoRequestMessage);

        final MchoiceUssdResponse mchoiceUssdResponse = new MchoiceUssdResponse();
        mchoiceUssdResponse.setCorrelationId("12352465436");
        mchoiceUssdResponse.setStatusCode("2000");
        mchoiceUssdResponse.setStatusDescription("Success");
        resp.getWriter().write(gson.toJson(mchoiceUssdResponse));
        resp.getWriter().flush();
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

    public static void main(String[] args) throws Exception {
        Server server = new Server(8000);

        final ServletContextHandler servletContextHandler = new ServletContextHandler();

        ServletHolder servlet = new ServletHolder(new SampleServer());
        servletContextHandler.addServlet(servlet, "/*");
        server.setHandler(servletContextHandler);
        server.start();
        server.join();
    }

}