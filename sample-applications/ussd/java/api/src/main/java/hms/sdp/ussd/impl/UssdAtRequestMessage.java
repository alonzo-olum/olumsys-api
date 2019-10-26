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

import hms.sdp.ussd.MchoiceUssdMessage;

/**
 * $LastChangedDate$
 * $LastChangedBy$
 * $LastChangedRevision$
 */
public class UssdAtRequestMessage implements MchoiceUssdMessage {

    private String version;
    private String correlationId;
    private String conversationId;
    private String address;
    private String message;
    private String sessionTermination;

    private transient String shortcode;

    @Override
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    @Override
    public String getConversationId() {
        return conversationId;
    }

    public void setConversationId(String conversationId) {
        this.conversationId = conversationId;
    }

    @Override
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getShortcode() {
        return shortcode;
    }

    public void setShortcode(String shortcode) {
        this.shortcode = shortcode;
    }

    @Override
    public boolean isSessionTermination() {
        return Boolean.parseBoolean(sessionTermination);
    }

    public void setSessionTermination(boolean sessionTermination) {
        this.sessionTermination = String.valueOf(sessionTermination);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("UssdAtRequestMessage");
        sb.append("{version='").append(version).append('\'');
        sb.append(", correlationId='").append(correlationId).append('\'');
        sb.append(", conversationId='").append(conversationId).append('\'');
        sb.append(", address='").append(address).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append(", shortcode='").append(shortcode).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
