package org.my

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
package object MR {
 implicit def stringToText(s:String):Text = new Text(s) 	  
 implicit def intToIntWritable(i:Int):IntWritable = new IntWritable(i) 
 implicit def IntWritable2Int(l: IntWritable): Int = l.get()
 implicit def textToString(t:Text):String = t.toString()
}