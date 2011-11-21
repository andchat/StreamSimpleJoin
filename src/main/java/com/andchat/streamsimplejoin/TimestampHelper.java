/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.andchat.streamsimplejoin;

import java.util.concurrent.TimeUnit;

/**
 *
 * @author s0902901
 */
public class TimestampHelper {
    
    public static String MilisecondsToTime(long millis)
    {
        return String.format("%d:%d:%d %d", 
            TimeUnit.MILLISECONDS.toHours(millis) - 
            TimeUnit.DAYS.toHours(TimeUnit.MILLISECONDS.toDays(millis)),    
            TimeUnit.MILLISECONDS.toMinutes(millis) - 
            TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millis)),
            TimeUnit.MILLISECONDS.toSeconds(millis) - 
            TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)),
            millis - 
            TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(millis)));
    }    
}
