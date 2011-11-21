/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.andchat.streamsimplejoin;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.Random;


/**
 *
 * @author s0902901
 */
public abstract class TupleGeneratorSpout implements IRichSpout {
    SpoutOutputCollector _collector;
    TopologyContext _context;
    Random _rand;    
    int _sleepInterval;
    
    public TupleGeneratorSpout (int sleepInterval)
    {
        _sleepInterval = sleepInterval;
    }
    
    @Override
    public boolean isDistributed()
    {
        return true;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _context = context;
        _rand = new Random();
    }

    public void nextTuple() {
        Utils.sleep(_sleepInterval);
       
        final Random rand = new Random();
        _collector.emit(new Values(rand.nextInt(30), 
                _context.getThisComponentId() + "col1value", 
                _context.getThisComponentId() + "col2value", 
                _context.getThisComponentId() + "col3value", 
                System.currentTimeMillis()));
    }    
    
    @Override
    public void close() {        
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    /*
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", 
                "col1", 
                "col2", 
                "col3",
                "timestamp"));
    }  
    */
}
