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

package com.dataartisans.blogpost.eventtime.java;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Main class of the sample application.
 * This class constructs and runs the data stream program. 
 */
public class Application {

    /**
     * Main entry point.
     */
    public static void main(String[] args) throws Exception {
        
        // create environment and configure it
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerType(Statistic.class);
        env.registerType(SensorReading.class);
        
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        
        // create a stream of sensor readings, assign timestamps, and create watermarks
        DataStream<SensorReading> readings = env
                .addSource(new SampleDataGenerator())
                .assignTimestamps(new ReadingsTimestampAssigner());
        
        // path (1) - low latency event-at a time filter
        readings
                .filter(reading -> reading.reading() > 100.0)
                .map( reading -> "-- ALERT -- Reading above threshold: " + reading )
                .print();
        
        // path (2) - processing time windows: Compute max readings per sensor group
        
        // because the default stream time is set to Event Time, we override the trigger with a
        // processing time trigger
        
        readings
                .keyBy( reading -> reading.sensorGroup() )
                .window(TumblingTimeWindows.of(Time.seconds(5)))
                .trigger(ProcessingTimeTrigger.create())
                .fold(new Statistic(), (curr, next) ->
                        new Statistic(next.sensorGroup(), next.timestamp(), Math.max(curr.value(), next.reading())))
                
                .map(stat -> "PROC TIME - max for " + stat)
                .print();
        
        // path (3) - event time windows: Compute average reading over sensors per minute
        
        // we use a WindowFunction here, to illustrate how to get access to the window object
        // that contains bounds, etc.
        // Pre-aggregation is possible by adding a pre-aggregator ReduceFunction
        
        readings
                // group by, window and aggregate 
                .keyBy(reading -> reading.sensorId() )
                .timeWindow(Time.minutes(1), Time.seconds(10))
                .apply(new WindowFunction<SensorReading, Statistic, String, TimeWindow>() {

                    @Override
                    public void apply(String id, TimeWindow window, Iterable<SensorReading> values, Collector<Statistic> out) {
                        int count = 0;
                        double agg = 0.0;
                        for (SensorReading r : values) {
                            agg += r.reading();
                            count++;
                        }
                        out.collect(new Statistic(id, window.getStart(), agg / count));
                    }
                })
                
                .map(stat -> "EVENT TIME - avg for " + stat)
                .print();
        
        env.execute("Event time example");
    }
    
    private static class ReadingsTimestampAssigner implements TimestampExtractor<SensorReading> {

        private static final long MAX_DELAY = 12000;
        
        private long maxTimestamp;
        
        @Override
        public long extractTimestamp(SensorReading element, long currentTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.timestamp());
            return element.timestamp();
        }

        @Override
        public long extractWatermark(SensorReading element, long currentTimestamp) {
            return Long.MIN_VALUE;
        }

        @Override
        public long getCurrentWatermark() {
            return maxTimestamp - MAX_DELAY;
        }
    }
}
