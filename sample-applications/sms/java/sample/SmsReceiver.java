/*
 *   (C) Copyright 2009-2010 hSenid Software International (Pvt) Limited.
 *
 *   hSenid Software International (Pvt) Limited retains all title to and intellectual
 *   property rights in these materials.
 *
 *
 */

import hsenidmobile.sdp.rest.servletbase.MchoiceAventuraMessagingException;
import hsenidmobile.sdp.rest.servletbase.MchoiceAventuraResponse;
import hsenidmobile.sdp.rest.servletbase.MchoiceAventuraSmsMessage;
import hsenidmobile.sdp.rest.servletbase.MchoiceAventuraSmsMoServlet;
import hsenidmobile.sdp.rest.servletbase.MchoiceAventuraSmsSender;
import java.net.URL;

public class SmsReceiver extends MchoiceAventuraSmsMoServlet {

    /**
     * This is used to simulate sms sending and receiving.
     * You can define your application logic inside the "onMessage" method.
     * Note :- Mobile numbers will be encrypted. 
     */
    private MchoiceAventuraSmsSender sender;
    
    @Override
    public void init() {

        try {
            super.init();
            sender = new MchoiceAventuraSmsSender(new URL("http://127.0.0.1:8008/simulator"), "TEST",
                    "test123");
        } catch (Exception e) {
            System.out.println("Url format is wrong, check the url again.");
            e.printStackTrace();
        }
    }
    
    @Override
    protected void onMessage(MchoiceAventuraSmsMessage message) {
        //your logic goes here......

        System.out.println("========== Mobile Originated message received ==========");
        System.out.println("Address = " + message.getAddress());
        System.out.println("Message = " + message.getMessage());
        System.out.println("Correlator = " + message.getCorrelator());
        System.out.println("Version = " + message.getVersion());
        System.out.println("========================================================");

        try {
            // send message to a single destination
            String sendingMessage = "Test message received. Thank you.";
            String sendingAddress = message.getAddress();

            System.out.println("Start sending sms message[" + sendingMessage + "] to [" + sendingAddress + "]");

            MchoiceAventuraResponse response = sender.sendMessage(sendingMessage, sendingAddress);
            if (response.isSuccess()) {
                System.out.println("Message [" + message + "] sent successfully to [" + sendingAddress + "].");
            } else {
                System.out.println("Failed to send message due to [" + response.getStatusCode() + "]["
                        + response.getStatusMessage() + "].");
            }
        } catch (MchoiceAventuraMessagingException e) {
            System.out.println("Exception occurred. Something wrong with message or connection ....");
            e.printStackTrace();
        }

    }
}
