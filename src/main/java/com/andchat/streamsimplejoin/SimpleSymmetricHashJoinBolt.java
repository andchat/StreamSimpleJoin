/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.andchat.streamsimplejoin;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Symmetric Hash Join
 * @author s0902901 Chatzistergiou Andreas
 * 
 * Remarks
 * - Tuple eviction is triggered each time a new tuple arrives.
 * - Tuples are stored in a queue to allow constant time tuple eviction.
 * - On top of that queue two hash tables are built (one for each operand)  
 *   to allow constant time probing.
 * - Each entry of the hash table is the tuple key along with a queue of tuple 
 *   to allow storing tuples with the same key.
 * - A tuple is only acknowledged when it is evicted.
 * 
 * Requirements
 * - Tuple timeout period has to be greater to window size, otherwise tuples  
 *   will start to expire before the window time is over.
 * - Memory for a single task has to be greater than F(S1*R1*T + S2*R2*T). Where:
 *   R1 is the rate of incoming tuples of stream 1.
 *   R2 is the rate of incoming tuples of stream 2.
 *   S1 Size of the tuple of stream 1.
 *   S2 Size of the tuple of stream 2.
 *   T is the window size in seconds.
 *   F Fudge factor to take into account the additional overhead of hash tables.
 * 
 * Questions
 * - What if a tuple is valid during the eviction but becomes invalid during 
 *   join (a few milliseconds later)?
 * 
 * ToDo
 * - Probably the tuple queue has to be split into to separate queues to avoid 
 *   the overhead of source info for each tuple.
 *     
 */
public class SimpleSymmetricHashJoinBolt implements IRichBolt {
    private TopologyContext _context;
    private OutputCollector _collector;
    private Map _conf;
    
    private Fields _outFields;
    private Map<String, GlobalStreamId> _fieldLocations;
    
    private long _windowSizeMillis;
    
    private GlobalStreamId _leftStream;
    private GlobalStreamId _rightStream;
    
    private HashMap<Object, Queue<Tuple>> _leftTableHashMap;
    private HashMap<Object, Queue<Tuple>> _rightTableHashMap;
    
    private LinkedList <TupleSourcePair> _tupleQueue;
    
    public SimpleSymmetricHashJoinBolt(Fields outFields, long windowSizeMillis)
    {
        _outFields = outFields;
        _windowSizeMillis = windowSizeMillis;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _conf = conf;
        _context = context;
        _collector = collector;

        // Setup sources
        if (context.getThisSources().size() != 2)
            throw new RuntimeException(this.getClass().getName() + 
                    " only supports join between two inputs.");

        Set<GlobalStreamId> sources = context.getThisSources().keySet();
        
        Iterator sourcesIterator = sources.iterator();

        _leftStream = (GlobalStreamId)sourcesIterator.next();
        _rightStream = (GlobalStreamId)sourcesIterator.next();
       
        _fieldLocations = new HashMap<String, GlobalStreamId>();
        
        _tupleQueue = new LinkedList<TupleSourcePair>();
        _leftTableHashMap = new HashMap<Object, Queue<Tuple>>();
        _rightTableHashMap = new HashMap<Object, Queue<Tuple>>();
        
        // Output fields
        for(GlobalStreamId source: sources) {
            Fields fields = context.getComponentOutputFields
                    (source.get_componentId(), source.get_streamId());
            
            for(String outfield: _outFields) {
                for(String sourcefield: fields) {
                    if(outfield.equals(sourcefield)) {
                        _fieldLocations.put(outfield, source);
                    }
                }
            }
        }
    }
    
    private void evictExpiredTuples()
    {
        // Evict expired tuples
        while(true)
        {
            if (_tupleQueue.size() == 0)
                break;
            
            TupleSourcePair topTupleInfo = _tupleQueue.peek();
            
            long windowLeftEdge = System.currentTimeMillis() - _windowSizeMillis;
            long topTupleTimestamp = (Long)topTupleInfo.getTuple()
                    .getValueByField("timestamp");
            
            if (topTupleTimestamp >= windowLeftEdge)
                break;
            
            String topTupleId = topTupleInfo.getTuple()
                    .getValueByField("id").toString();
            
            _tupleQueue.remove();
            
            HashMap<Object, Queue<Tuple>> storeTable;
            if (topTupleInfo.getSource().equals(_leftStream))
            {
                storeTable = _leftTableHashMap;
            }
            else if (topTupleInfo.getSource().equals(_rightStream))
            {
                storeTable = _rightTableHashMap;
            }
            else
            {
                throw new RuntimeException("Unknown source");            
            }            
            
            // Remove
            Queue<Tuple> commonKeyTupleQueue = storeTable.get(topTupleId);
            commonKeyTupleQueue.poll();
            
            if (commonKeyTupleQueue.isEmpty())
            {
                storeTable.remove(topTupleId);
            }
            
            // Processing of that tuple is finished
            _collector.ack(topTupleInfo.getTuple());
            
            // Debug
            if (_conf.get("topology.debug").toString().equals("true"))
            {
                Logger.log(_context, "TUPLE EVICTED", topTupleId, 
                        topTupleTimestamp, _tupleQueue.size() + " tuples in memory");
            }            
        }        
    }
    
    private void storeTuple(GlobalStreamId source, Object tupleId, Tuple tuple, 
            HashMap<Object, Queue<Tuple>> storeTable)
    {
        Queue<Tuple> commonKeyTupleQueue;
        
        if (!storeTable.containsKey(tupleId))
        {
            commonKeyTupleQueue = new LinkedList<Tuple>();
            storeTable.put(tupleId, commonKeyTupleQueue);
        }
        else
        {
            commonKeyTupleQueue = storeTable.get(tupleId);
        }
        
        // Enqueue tuple
        TupleSourcePair tupleInfo = new TupleSourcePair(tuple, source);
        
        _tupleQueue.offer(tupleInfo);
        commonKeyTupleQueue.add(tuple);        
        
        // Debug
        if (_conf.get("topology.debug").toString().equals("true"))
        {
            Logger.log(_context, "TUPLE RECEIVED", tupleId.toString(), 
                    (Long)tuple.getValueByField("timestamp"), 
                    "Source:" + source
                    + " ," + _tupleQueue.size() + " tuples in memory");
        }        
    }

    @Override
    public void execute(Tuple tuple) {
        // Evict expired tuples
        evictExpiredTuples();
        
        // Get source stream
        GlobalStreamId source = 
                new GlobalStreamId(tuple.getSourceComponent(), 
                        tuple.getSourceStreamId());
        
        // Get key & timestamp
        String tupleId = tuple.getValueByField("id").toString();
        long tupleTimestamp = (Long)tuple.getValueByField("timestamp");
        
        HashMap<Object, Queue<Tuple>> probeTable;
        HashMap<Object, Queue<Tuple>> storeTable;
        
        if (source.equals(_leftStream))
        {
            storeTable = _leftTableHashMap;
            probeTable = _rightTableHashMap;
        }
        else if (source.equals(_rightStream))
        {
            storeTable = _rightTableHashMap;
            probeTable = _leftTableHashMap;
        }
        else
        {
            throw new RuntimeException("Unknown source");            
        }

        // 1. Store new tuple 
        storeTuple(source, tupleId, tuple, storeTable);
        
        // 2. Prompt the other table
        if (probeTable.containsKey(tupleId))
        {
            // Perform join
            for(Tuple probeTuple : probeTable.get(tupleId))
            {
                ArrayList<Tuple> anchors = new ArrayList<Tuple>();
                anchors.add(tuple);
                anchors.add(probeTuple);
                
                List<Object> joinResult = new ArrayList<Object>();

                for(String outField: _outFields) {
                    GlobalStreamId loc = _fieldLocations.get(outField);
                    
                    if (loc.equals(source))
                    {
                        joinResult.add(tuple.getValueByField(outField));
                    }
                    else
                    {
                        joinResult.add(probeTuple.getValueByField(outField));
                    }
                }
                
                // Emit result
                _collector.emit(anchors, joinResult);       
                
                // Debug
                if (_conf.get("topology.debug").toString().equals("true"))
                {
                    Logger.log(_context, "TUPLE JOINED", tupleId, tupleTimestamp);
                }
            }
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /*
        ArrayList fields = new ArrayList();
       
        System.out.println("*****" + fields.toString());
        
        fields.addAll(_context.getComponentOutputFields
                (leftSource.get_componentId(), leftSource.get_streamId()).toList());

        System.out.println("*****" + fields.toString());
        
        fields.addAll(_context.getComponentOutputFields
                (rightSource.get_componentId(), rightSource.get_streamId()).toList());
        
        System.out.println("*****" + fields.toString());
        
        declarer.declare(new Fields(fields));
        */
        declarer.declare(_outFields);
    }    
}
