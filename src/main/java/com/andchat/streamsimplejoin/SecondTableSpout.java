/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.andchat.streamsimplejoin;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

/**
 *
 * @author s0902901
 */
public class SecondTableSpout extends TupleGeneratorSpout  {
    public SecondTableSpout (int sleepInterval)
    {
        super(sleepInterval);
    }    
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", 
                "col1_2", 
                "col2_2", 
                "col3_2",
                "timestamp"));
    }    
}
