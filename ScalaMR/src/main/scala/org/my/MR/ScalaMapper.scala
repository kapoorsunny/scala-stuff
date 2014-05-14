package org.my.MR

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable

class ScalaMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  type Context = Mapper[org.apache.hadoop.io.LongWritable,org.apache.hadoop.io.Text,org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable]#Context

 
 protected override def map(key: LongWritable, value: Text, context:Context) {
    value split(" ") foreach (context.write(_, 1))
  }
  
}

