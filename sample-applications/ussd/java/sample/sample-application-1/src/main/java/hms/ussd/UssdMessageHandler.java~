
/*
 * (C) Copyright 2010-2011 hSenid Software International (Pvt) Limited.
 * All Rights Reserved.
 *
 * These materials are unpublished, proprietary, confidential source code of
 * hSenid Software International (Pvt) Limited and constitute a TRADE SECRET
 * of hSenid Software International (Pvt) Limited.
 *
 * hSenid Software International (Pvt) Limited retains all title to and intellectual
 * property rights in these materials.
 */

package hms.ussd;

import hms.sdp.ussd.MchoiceUssdException;
import hms.sdp.ussd.MchoiceUssdMessage;
import hms.sdp.ussd.MchoiceUssdResponse;
import hms.sdp.ussd.MchoiceUssdTerminateMessage;
import hms.sdp.ussd.client.MchoiceUssdReceiver;
import hms.sdp.ussd.client.MchoiceUssdSender;
import org.apache.log4j.Logger;

import javax.servlet.ServletException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class UssdMessageHandler extends MchoiceUssdReceiver {

    private static Properties properties = new Properties();
    private static String receiverAddress = null;
    private static String appId = null;
    private static String password = null;
    private ConcurrentHashMap<String, User> userMap;

    public void init() throws ServletException {
        super.init();
        userMap = new ConcurrentHashMap<String, User>();
        loadProperties();
    }

    /**
     * This method receives the MchoiceUssdMessage and extract the fields from it
     * in order to process the message
     *
     * @param ussdMessage received ussd message
     */
    @Override
    public void onMessage(MchoiceUssdMessage ussdMessage) {

        String reply;
        if(userMap.get(ussdMessage.getConversationId()) == null) {
            userMap.put(ussdMessage.getConversationId(), new User());
        }
        try {
            storeUserDetails(userMap.get(ussdMessage.getConversationId()), ussdMessage.getMessage());
            reply = generateReply(userMap.get(ussdMessage.getConversationId()));
        } catch(NumberFormatException e) {
            reply = "Incorrect value for age \nEnter your age";
            System.out.println("Age provided is not a number");
            e.printStackTrace();
        } catch (Exception e) {
            reply = "Incorrect value for gender \nYour gender \n1. Male \n2. Female";
            e.printStackTrace();
        }

        try {
            MchoiceUssdSender ussdSender = new MchoiceUssdSender(receiverAddress, appId, password);
            MchoiceUssdResponse mchoiceUssdResponse =
                                ussdSender.sendMessage(reply, ussdMessage.getAddress(), ussdMessage.getConversationId(), false);
            System.out.println("-------ussd response-------");
            System.out.println(mchoiceUssdResponse);

        } catch (MchoiceUssdException e) {
            e.printStackTrace();
        }

    }

    /**Ussd session is terminated when the application receives UssdTerminateMessage
     * So the user details are removed from the map
     *
     * @param mchoiceUssdTerminateMessage received ussd terminate message
     */
    @Override
    public void onSessionTerminate(MchoiceUssdTerminateMessage mchoiceUssdTerminateMessage) {
        System.out.println("-----terminate message-----");
        userMap.remove(mchoiceUssdTerminateMessage.getConversationId());
    }

/**  Store the details provided by the user in ussd message
     *
     * @param user user
     * @param message content of the ussd message
     */
    private void storeUserDetails(User user, String message) throws Exception {

        switch (user.getStatus()) {
            case User.STATUS_INITIAL:
                break;
            case User.STATUS_NAME:
                user.setName(message);
                break;
            case User.STATUS_AGE:
                user.setAge(Integer.parseInt(message));
                break;
            case User.STATUS_GENDER:
                if(message.contentEquals("1")) {
                    user.setGender('M');
                } else if(message.contentEquals("2")) {
                    user.setGender('F');
                } else {
                    throw new Exception("Incorrect value for gender");
                }
                break;
        }
    }

    /**
     * Check user's current status and generate the reply message
     * according to the current status
     * then sets the next status
     *
     * @param user user
     * @return the response message according to the current user status
     */
    private String generateReply(User user) {

        String reply = "";
        int nextStatus = 0;
        switch (user.getStatus()) {

            case (User.STATUS_INITIAL):
                reply = "Enter your name";
                nextStatus = User.STATUS_NAME;
                break;
            case (User.STATUS_NAME):
                reply = "Enter your age";
                nextStatus = User.STATUS_AGE;
                break;
            case (User.STATUS_AGE):
                reply = "Enter your gender \n1. Male \n2. Female";
                nextStatus = User.STATUS_GENDER;
                break;
            case (User.STATUS_GENDER):
            case (User.STATUS_DETAILS):
                reply = "Your details \nname: "+user.getName()+"\nage: "+user.getAge()+"\ngender: "+user.getGender();
                nextStatus = User.STATUS_DETAILS;
                break;
        }
        user.setStatus(nextStatus);
        return reply;
    }

    /**Read appId, Password and simualtor's address from the properties file
     * and assign them to the relevant variables
     *
     */
    private void loadProperties(){
        InputStream inputStream = this.getClass().getResourceAsStream("/application.properties");
        try {
            properties.load(inputStream);
            receiverAddress = properties.getProperty("ussd.receiver.address");
            appId = properties.getProperty("appId.of.the.application");
            password = properties.getProperty("password.of.the.application");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try{
                inputStream.close();
            } catch(IOException ignored) { }
        }
    }
}