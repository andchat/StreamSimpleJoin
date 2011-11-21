/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.andchat.streamsimplejoin;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 *
 * @author s0902901
 */
public class SimpleJoinTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(1, new FirstTableSpout(100), 4);        
        builder.setSpout(2, new SecondTableSpout(100), 4);        
        
        builder.setBolt(3, new SimpleSymmetricHashJoinBolt(
                new Fields("id","col1_1","col2_1","col3_1","col1_2","col2_2","col3_2")
                , 3000), 2)
                .fieldsGrouping(1, new Fields("id"))
                .fieldsGrouping(2, new Fields("id"));
        
        // Create a local cluster
        Config conf = new Config();
        conf.setDebug(true);
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
        
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();    
        }
    }    
}
