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

/**
 * Created by IntelliJ IDEA.
 * User: hms
 * Date: 5/23/11
 * Time: 12:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class User {

    public static final int STATUS_INITIAL = 1;
    public static final int STATUS_NAME = 2;
    public static final int STATUS_AGE = 3;
    public static final int STATUS_GENDER = 4;
    public static final int STATUS_DETAILS = 5;

    private String name;
    private int age;
    private char gender;
    private int status;

    /**
     * when a user is created STATUS_INITIAL is assigned as user status
     */
    public User() {
        this.status = STATUS_INITIAL;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public char getGender() {
        return gender;
    }

    public void setGender(char gender) {
        this.gender = gender;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
