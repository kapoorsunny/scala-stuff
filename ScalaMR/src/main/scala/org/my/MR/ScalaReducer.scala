package org.my.MR

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._

class ScalaReducer extends Reducer[Text, IntWritable, Text,IntWritable]{
	type Context =  Reducer[Text, IntWritable, Text, IntWritable]#Context
	
	
   protected override def reduce(key: Text, value: java.lang.Iterable[IntWritable], context:Context): Unit = {
      context.write(key, value.reduceLeft(_ + _))
    }
}