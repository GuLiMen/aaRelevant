package com.qtech.bigdata.util;

import org.junit.Test;

import java.util.Calendar;

public class date_D_N {

    public static String Date_D(){
        Calendar cal = Calendar.getInstance();
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DATE) - 0;
        return month+"-"+day+"-D";
    }

    public static String Date_N(){
        Calendar cal = Calendar.getInstance();
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DATE) - 0;
        return month+"-"+day+"-N";
    }
}
