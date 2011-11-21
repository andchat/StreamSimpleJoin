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
public class FirstTableSpout extends TupleGeneratorSpout {
    
    public FirstTableSpout (int sleepInterval)
    {
        super(sleepInterval);
    }    
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", 
                "col1_1", 
                "col2_1", 
                "col3_1",
                "timestamp"));
    }     
}
