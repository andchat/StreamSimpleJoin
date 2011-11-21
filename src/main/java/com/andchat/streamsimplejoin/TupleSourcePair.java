/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.andchat.streamsimplejoin;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Tuple;

/**
 *
 * @author s0902901
 */
public class TupleSourcePair {
    private Tuple _tuple;
    private GlobalStreamId _source;
    
    public TupleSourcePair (Tuple tuple, GlobalStreamId source)
    {
        _tuple = tuple;
        _source = source;
    }
    
    public Tuple getTuple()
    {
        return _tuple;
    }
    
    public GlobalStreamId getSource()
    {
        return _source;
    }
}


