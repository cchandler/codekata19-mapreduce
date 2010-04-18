package codekata19;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class CodeKata19 extends Configured implements Tool
{
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text currentPermutation = new Text();
    public static java.util.Map<String,String> dictionary = new HashMap<String,String>();
    public final static char[] letters = {'a','b','c','d','e','f','g','h','i', 'j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};
    
    public void configure(JobConf job) {
      String wordlist = job.get("wordlist");
      CodeKata19.loadDictionary(wordlist);
    }
    
  
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
        String comp = value.toString().trim();
        char[] array = value.toString().toCharArray();

        for(int i = 0; i < array.length; i++)
        {
          for(char c : CodeKata19.Map.letters)
          {
            char[] newString = new char[array.length];
            System.arraycopy( array, 0, newString, 0, array.length );
            newString[i] = c;
            String newString1 = String.valueOf(newString);
            if(CodeKata19.Map.dictionary.containsKey( newString1 ) && !newString1.equalsIgnoreCase(comp))
            {
              currentPermutation.set(newString1);
              output.collect(value, currentPermutation);
            }
          }
        }
        
      }
  }
  
  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    
    java.util.Map<String,String> alreadySeen = new HashMap<String,String>();
    
    while(values.hasNext())
    {
      String s = values.next().toString();
      if(!alreadySeen.containsKey(s))
      {
        alreadySeen.put(s,"");
      }
    }
    
    StringBuffer buffer = new StringBuffer();
    for(String s : alreadySeen.keySet())
    {
      buffer.append(s);
      buffer.append(",");
    }
    
    buffer.append("|-1|WHITE| ");
      
    Text combined_output = new Text();
    combined_output.set(buffer.toString());
    output.collect(key, combined_output);
      
    }
  }
  
  public List<String> buildPermutations(String s)
  {
    char[] array = s.toCharArray();
    List<String> results = new LinkedList<String>();
    
    for(int i = 0; i < array.length; i++)
    {
      for(char c : CodeKata19.Map.letters)
      {
        char[] newString = new char[array.length];
        System.arraycopy( array, 0, newString, 0, array.length );
        newString[i] = c;
        String newString1 = String.valueOf(newString);
        if(CodeKata19.Map.dictionary.containsKey( newString1 ) && !newString1.equals(s))
        {
          results.add(newString1);
        }
      }
    }
    
    return results;
  }
  
  public static boolean loadDictionary(String file)
  {
    System.out.println(CodeKata19.Map.dictionary);
    boolean result = false;
    try
    {
      FileInputStream fstream = new FileInputStream(file);
      DataInputStream in = new DataInputStream(fstream);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String strLine;
      while ((strLine = br.readLine()) != null)   {
        CodeKata19.Map.dictionary.put(strLine, "");
      }
      
      in.close();
      result = true;
    }
    catch(Exception e)
    {
      System.err.println("Error reading in dictionary!");
      System.err.println(e);
    }
    
    return result;
  }
	
	public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), CodeKata19.class);
    conf.setJobName("codekata19");
  
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
  
    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);
  
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
  
    DistributedCache.addCacheFile( new Path(args[1]).toUri(), conf);
    conf.set("wordlist", args[1]);
  
    FileInputFormat.setInputPaths(conf, new Path(args[1]));
    FileOutputFormat.setOutputPath(conf, new Path(args[2]));
  
    JobClient.runJob(conf);
    return 0;
  }
	
	public static void main(String[] args) throws Exception {
	  int res = 0;
	  if(args[0].equals("graph"))
	  {
	    res = ToolRunner.run(new Configuration(), new CodeKata19(), args);
	  }
	  else
	  {
	    res = ToolRunner.run(new Configuration(), new CodeKata19Search(), args);
	  }
    
    System.exit(res);
  }
}
