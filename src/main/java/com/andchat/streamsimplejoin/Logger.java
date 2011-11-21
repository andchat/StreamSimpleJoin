/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.andchat.streamsimplejoin;

import backtype.storm.task.TopologyContext;

/**
 *
 * @author s0902901
 */
public class Logger {
    public static void log (TopologyContext context, String action, 
            String tupleId, Long tupleTimeStamp, String addInfo)
    {
        System.out.println(action
            + " [" + TimestampHelper.MilisecondsToTime(System.currentTimeMillis()) + "]"    
            + " ,Task:" + context.getThisTaskId() 
            + " ,Component:" + context.getThisComponentId() 
            + " ,tuple: id " + tupleId
            + " [" + TimestampHelper.MilisecondsToTime(tupleTimeStamp) + "]" 
            +  (addInfo.length() > 0?" ,":"") + addInfo
            ) ; 
    }
    
    public static void log (TopologyContext context, String action, 
            String tupleId, Long tupleTimeStamp)
    {
        Logger.log(context, action, tupleId, tupleTimeStamp, "");
    }    
}
