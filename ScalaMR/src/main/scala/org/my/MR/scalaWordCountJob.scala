package org.my.MR

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.util.Tool
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.fs.Path

object scalaWordCountJob extends Configured with Tool{
  
   implicit def toPath(s: String): Path = new Path(s)
  
 def run(args: Array[String]): Int = {
    val conf = getConf
    conf.setQuietMode(false)

    val job: Job = new Job(conf, "Word Count")

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[ScalaMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])

//    job.setCombinerClass(classOf[ScalaReducer])
    job.setReducerClass(classOf[ScalaReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[LongWritable])

    for (i <- 0 to args.length - 2) {
      FileInputFormat.addInputPath(job, args.apply(i))
    }

    FileOutputFormat.setOutputPath(job, args.last)

    job.waitForCompletion(true) match {
      case true => 0
      case false => 1
    }
  }
 
  def main(args: Array[String]) {
    System.exit(ToolRunner.run(this, args))
  }

}