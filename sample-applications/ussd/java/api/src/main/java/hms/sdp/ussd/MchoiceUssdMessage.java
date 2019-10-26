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
package hms.sdp.ussd;

/**
 * $LastChangedDate$
 * $LastChangedBy$
 * $LastChangedRevision$
 */
public interface MchoiceUssdMessage {

    public static final String USSD_MESSAGE_TYPE = "X-Message-type";

    public static final String USSD_MESSAGE = "X-USSD-Message";
    public static final String USSD_TERMINATE_MESSAGE = "X-USSD-Terminate-Message";
    public static final String USSD_ALIVE_MESSAGE = "X-USSD-Alive-Message";
    public static final String REQUEST_VERSION = "X-Requested-Version";
    public static final String CONVERSATION = "X-Requested-Conversation-ID";
    public static final String ENCODING = "X-Requested-Encoding";

    String getVersion();

    String getConversationId();

    String getAddress();

    String getMessage();

    String getCorrelationId();

    boolean isSessionTermination();
}
