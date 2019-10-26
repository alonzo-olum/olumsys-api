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
package hms.sdp.ussd.impl;

/**
 * $LastChangedDate$
 * $LastChangedBy$
 * $LastChangedRevision$
 */
public class UssdAoRequestMessage {

    private String conversationId;
    private String address;
    private String message;
    private String sessionTermination;

    public boolean getSessionTermination() {
        return Boolean.parseBoolean(sessionTermination);
    }

    public void setSessionTermination(boolean sessionTermination) {
        this.sessionTermination = String.valueOf(sessionTermination);
    }

    public String getConversationId() {
        return conversationId;
    }

    public void setConversationId(String conversationId) {
        this.conversationId = conversationId;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("UssdAoRequestMessage");
        sb.append("{conversationId='").append(conversationId).append('\'');
        sb.append(", address='").append(address).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append(", sessionTermination=").append(sessionTermination);
        sb.append('}');
        return sb.toString();
    }
}
