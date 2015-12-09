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

import java.util.Random
import java.util.concurrent.{TimeUnit, Executors, ScheduledExecutorService}

import com.dataartisans.blogpost.eventtime.scala.Application.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction

/**
 * A sample data generator of sensor readings.
 */
class SampleDataGenerator extends RichParallelSourceFunction[SensorReading] {

  val numSensors: Int = 1000
  val numSensorGroups: Int = 80
  val baseInterval: Int = 5000
  val maxDelay: Int = 6000
  val maxRegularReading: Double = 100.0
  val fractionOfExceedingReadings: Double = 0.0005
  
  @volatile
  private var running: Boolean = true
  
  def run(ctx: SourceFunction.SourceContext[SensorReading]) {
    val rnd = new Random()
    val exec = Executors.newScheduledThreadPool(2)
    val idOffset = getRuntimeContext.getIndexOfThisSubtask * numSensors
    
    try {
      while (running) {
        val baseTimestamp = System.currentTimeMillis()
        
        for (i <- 1 to numSensors) {
          val shiftInInterval = rnd.nextInt(baseInterval) - baseInterval
          
          val reading = if (rnd.nextDouble() < fractionOfExceedingReadings) 
                          maxRegularReading + rnd.nextDouble() * maxRegularReading
                        else
                           rnd.nextDouble() * maxRegularReading
          
          val sensorName = "sensor-%04d".format(i + idOffset)
          val groupName = "group-%03d".format(i % numSensorGroups)
          
          val delay = rnd.nextInt(maxDelay)
          val event = SensorReading(groupName, sensorName, baseTimestamp + shiftInInterval, reading)
          
          val runnable = new Runnable {
            override def run(): Unit = {
              ctx.collect(event)
            }
          }
          
          exec.schedule(runnable, delay, TimeUnit.MILLISECONDS)
        }
        
        Thread.sleep(baseInterval)
      }
    }
    finally {
      exec.shutdownNow()
    }
  }

  def cancel() {
    running = false
  }
}
