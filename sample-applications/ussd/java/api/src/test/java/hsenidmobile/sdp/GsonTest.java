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
import hms.sdp.ussd.impl.UssdAoRequestMessage;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;

/**
 * $LastChangedDate$
 * $LastChangedBy$
 * $LastChangedRevision$
 */
public class GsonTest {

    @Test
    public void testGson() {

        Gson gson = new Gson();
        final UssdAoRequestMessage ussdAoRequestMessage = new UssdAoRequestMessage();
        ussdAoRequestMessage.setAddress("1234132");
        ussdAoRequestMessage.setMessage("sdfs");
        final String json = gson.toJson(ussdAoRequestMessage);
//        gson.from
        System.out.println(json);

        final Calendar instance = Calendar.getInstance();
        instance.add(Calendar.DATE, 20);
        System.out.println(instance.getTimeInMillis());
        System.out.println(new Date(1305102248417l));
    }
}
