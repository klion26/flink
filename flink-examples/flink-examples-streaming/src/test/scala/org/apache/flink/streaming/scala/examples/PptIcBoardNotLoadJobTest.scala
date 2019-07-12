/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.scala.examples

import java.lang
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.{CoGroupedStreams, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.mutable

case class Event(tm: Long, e: String)

class PptIcBoardNotLoadJobTest {
  @Test
  def testCoGroup(): Unit = {
//    CoGroupJoinITCase.testResults = mutable.MutableList()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    val ppt = env.addSource(new SourceFunction[Event]() {
      def run(ctx: SourceFunction.SourceContext[Event]) {
        var tm = 1551023001000L // 2019-02-24 23:43:21
        val dot =  100 // 100毫秒打点一次
        while (true) {
          Thread.sleep(dot)
          ctx.collect(Event(tm, "a"))
          ctx.collect(Event(tm, "B"))
          ctx.collect(Event(tm, "C"))

          tm = tm + dot
        }
        // source is finite, so it will have an implicit MAX watermark when it finishes
      }
      def cancel() {}
    }).assignTimestampsAndWatermarks(new PptIcBoardNotLoadJobTest.PptTimestampExtractor)


    val sensor = env.addSource(new SourceFunction[Event]() {
      def run(ctx: SourceFunction.SourceContext[Event]) {
        var tm = 1551023001000L // 2019-02-24 23:43:21
        val dot =  12000 // 100毫秒打点一次
        while (true) {
          Thread.sleep(dot)
          ctx.collect(Event(tm, "a"))

          tm = tm + dot
        }
      }
      def cancel() {}
    }).assignTimestampsAndWatermarks(new PptIcBoardNotLoadJobTest.SensorTimestampExtractor2)

    val re = sensor.coGroup(ppt)
      .where(_.e)
      .equalTo(_.e)
      .window(SlidingEventTimeWindows.of(Time.of(10000, TimeUnit.MILLISECONDS),Time.of(5000, TimeUnit.MILLISECONDS)))
      .sideOutputTag(new OutputTag[CoGroupedStreams.TaggedUnion[Event, Event]]("abc"){})
      .apply(new CoFun)

    val soso = re.javaStream.asInstanceOf[SingleOutputStreamOperator[CoGroupedStreams.TaggedUnion[Event, Event]]]
    val sidoutput = soso.getSideOutput(new OutputTag[CoGroupedStreams.TaggedUnion[Event, Event]]("abc"){})
    sidoutput.print()
    env.execute("CoGroup Test")
  }
}

class CoFun extends CoGroupFunction[Event, Event, String] {
  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  override def coGroup(sensor: lang.Iterable[Event], ppt: lang.Iterable[Event], out: Collector[String]): Unit = {
    if (sensor.asScala.size > 0){
      //println("sensor data:"+ sensor.asScala.map(a => format.format(a.tm)).mkString("")+" sensor: " + sensor.asScala.size + ", ppt: " + ppt.asScala.size)
    }else{
      //println(" sensor: " + sensor.asScala.size + ", ppt: " + ppt.asScala.size)
    }

    if (ppt.asScala.size > 0) {
      val sorted = ppt.asScala.toList.sortBy(_.tm)
      val headTm = sorted.head.tm
      val tailTm = sorted.last.tm
      println(format.format(System.currentTimeMillis()) + " -- window started," + " elementStartTime:" + format.format(headTm) + ", endTime:" + format.format(tailTm) + ", sensor: " + sensor.asScala.size + ", ppt: " + ppt.asScala.size)

    }
  }
}


object PptIcBoardNotLoadJobTest {
  var testResults: mutable.MutableList[String] = null

  class PptTimestampExtractor extends AssignerWithPunctuatedWatermarks[Event] {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    override def extractTimestamp(element: Event, previousTimestamp: Long): Long = {
      //println("Ppt extract： "+format.format(element.tm))
      element.tm
    }

    override def checkAndGetNextWatermark(lastElement: Event,
                                          extractedTimestamp: Long): Watermark = {
      val wm = new Watermark(extractedTimestamp - 3000)
      //println("Ppt checkAndGet WM： "+format.format(wm.getTimestamp))
      wm
    }
  }

  class SensorTimestampExtractor2 extends AssignerWithPunctuatedWatermarks[Event] {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    override def extractTimestamp(element: Event, previousTimestamp: Long): Long = {
      println("Sensor extract： " + format.format(element.tm))
      element.tm
    }

    override def checkAndGetNextWatermark(lastElement: Event,
                                          extractedTimestamp: Long): Watermark = {
      val wm = new Watermark(extractedTimestamp - 3000)
      println("Sensor checkAndGet WM： " + format.format(wm.getTimestamp))
      wm
    }
  }

}
