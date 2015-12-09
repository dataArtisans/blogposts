/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.blogpost.eventtime.scala

import java.lang

import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.TimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.math._
import scala.collection.JavaConverters._

/**
 * Main class of the sample application.
 * This class constructs and runs the data stream program. 
 */
object Application {

  // --------------------------------------------------------------------------
  
  /**
   * An individual sensor reading, describing sensor id, sensor group id, reading, and timestamp.
   */
  case class SensorReading(sensorGroup: String, sensorId: String, timestamp: Long, reading: Double)

  /**
   * An aggregate of a sensor or group reading with a timestamp.
   */
  case class Statistic(id: String, timestamp: Long, value: Double)

  // --------------------------------------------------------------------------
  
  def main(args: Array[String]): Unit = {
    
    // create environment and configure it
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    // create a stream of sensor readings, assign timestamps, and create watermarks
    val readings: DataStream[SensorReading] = 
      env.addSource(new SampleDataGenerator())
         .assignTimestamps(new ReadingsTimestampAssigner())
         

    // path (1) - low latency event-at a time filter
    readings
      .filter(_.reading > 100.0)
      
      .map("-- ALERT -- Reading above threshold: " + _.reading)
      .print()

    // path (2) - processing time windows: Compute max readings per sensor group
    // because the default stream time is set to Event Time, we override the trigger with a
    // processing time trigger

    readings
      .keyBy(_.sensorGroup)
      .window(TumblingTimeWindows.of(Time.seconds(5)))
      .trigger(ProcessingTimeTrigger.create)
      .fold(Statistic("", 0L, 0.0), (curr: Statistic, next: SensorReading) => { 
         Statistic(next.sensorGroup, next.timestamp, max(curr.value, next.reading))
      })
      
      .map("PROC TIME - max for " + _)
      .print()

    // path (3) - event time windows: Compute average reading over sensors per minute
    // we use a WindowFunction here, to illustrate how to get access to the window object
    // that contains bounds, etc.
    // Pre-aggregation is possible by adding a pre-aggregator ReduceFunction

    readings
      .keyBy(_.sensorId)
      .timeWindow(Time.minutes(1), Time.seconds(10))
      .apply(new WindowFunction[SensorReading, Statistic, String, TimeWindow]() {
      
          override def apply(id: String,
                             window: TimeWindow,
                             values: lang.Iterable[SensorReading],
                             out: Collector[Statistic]): Unit = {
            
            val readings : Iterable[Double] = values.asScala.map(_.reading)
            val avg = readings.sum / readings.size
            
            out.collect(Statistic(id, window.getStart, avg))
          }
      })
      
      .map("EVENT TIME - avg for " + _)
      .print()

    env.execute("Event time example")
  }

  // --------------------------------------------------------------------------
  
  /**
   * A timestamp extractor that uses the "timestamp" field from sensor readings.
   * 
   * It also generates watermarks based on a simple heuristic that: Elements are never more than
   * 12 seconds late.
   */
  private class ReadingsTimestampAssigner extends TimestampExtractor[SensorReading] {

    private val MAX_DELAY = 12000L
    
    private var maxTimestamp= 0L

    def extractTimestamp(element: SensorReading, currentTimestamp: Long): Long = {
      maxTimestamp = max(maxTimestamp, element.timestamp)
      element.timestamp
    }

    def extractWatermark(element: SensorReading, currentTimestamp: Long) = Long.MinValue

    def getCurrentWatermark: Long = maxTimestamp - MAX_DELAY
  }
}
