package codekata19;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class CodeKata19Search extends Configured implements Tool
{
  static enum Counters { MORE_GRAY };
  
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      Text word = new Text();
      Text new_node_data = new Text();
      
        String id_data[] = value.toString().split("\t");
        String data[] = id_data[1].split("\\|");
        
        String edges[] = data[0].split(",");
        String distance = data[1];
        String color = data[2];
        String path = data[3];
        
        String longer_path = (path + "," + id_data[0]).trim();
        
        if(color.equalsIgnoreCase("GRAY"))
        {
          int new_distance = Integer.parseInt(distance) + 1;
          
          for(String s : edges)
          {
            word.set(s);
            new_node_data.set("NULL|" + new_distance + "|GRAY|" + longer_path);
            output.collect(word, new_node_data);
          }
          
          word.set(id_data[0]);
          new_node_data.set("NULL|" + new_distance + "|BLACK|" + path);
          output.collect(word,new_node_data);
        }

        word.set(id_data[0]);
        new_node_data.set(id_data[1]);
        output.collect(word,new_node_data);
        
    }
  }
  
  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
      int distance = -1;
      String edges[] = {"NULL"};
      String color = "WHITE";
      String path = " ";
      
      while(values.hasNext())
      {
        String data[] = values.next().toString().split("\\|");
        
        String node_edges[] = data[0].split(",");
        String node_distance = data[1];
        String node_color = data[2];
        String node_path = data[3];
       
        if(!node_edges[0].equalsIgnoreCase("NULL"))
        {
          edges = node_edges;
        }
        
        int new_distance = Integer.parseInt(node_distance);
        
        if((new_distance < distance && new_distance >= 0) || distance == -1)
        {
          distance = new_distance;
        }
        
        if(node_color.equals("GRAY") && color.equals("WHITE"))
        {
          color = "GRAY";
          path = node_path;
        }
        if(node_color.equals("BLACK"))
        {
          color = "BLACK";
          path = node_path;
        }
      }
      
      Text combined_output = new Text();
      StringBuffer buffer = new StringBuffer();
      for(String s : edges)
      {
        buffer.append(s);
        buffer.append(",");
      }
      combined_output.set(buffer.toString() + "|" + distance + "|" + color + "|" + path);
      output.collect(key, combined_output);
      
      if(color.equalsIgnoreCase("GRAY"))
      {
        //We are outputting a gray node
        reporter.incrCounter(Counters.MORE_GRAY, 1);
      }
      
    }
  }
  
  public int run(String[] args) throws Exception {
    int iterations = 0;
    boolean keep_going = true;
    String input_dir = args[1] + "/";
    String output_dir = args[2] + "/";
    String general_output_dir = args[2] + "/";
    
    while(keep_going)
    {      
      iterations++;
      
      JobConf conf = new JobConf(getConf(), CodeKata19Search.class);
      conf.setJobName("codekata19search");
  
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);
  
      conf.setMapperClass(Map.class);
      conf.setReducerClass(Reduce.class);
  
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
  
      FileInputFormat.setInputPaths(conf, new Path(input_dir));
      FileOutputFormat.setOutputPath(conf, new Path(output_dir));
  
      RunningJob job = JobClient.runJob(conf);
      org.apache.hadoop.mapreduce.Counter counter = job.getCounters().findCounter(CodeKata19Search.Counters.MORE_GRAY);
    
      //If more gray is > 0 then we run the job again
      if(counter.getValue() == 0)
      {
        System.out.println("No more gray nodes!");
        keep_going = false;
      }
      else
      {
        input_dir = output_dir;
        output_dir = general_output_dir + iterations + "/";
      }

    }    
    
    return 0;
  }
}