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

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A sample data generator of sensor readings.
 */
public class SampleDataGenerator extends RichParallelSourceFunction<SensorReading> {
    
    private static final int numSensors = 1000;

    private static final int numSensorGroups = 80;
    
    private static final int baseInterval = 5000;

    private static final int maxDelay = 6000;
    
    private static final double maxRegularReading = 100.0;
    
    private static final double fractionOfExceedingReadings = 0.0005;
    
    
    private volatile boolean running = true;
    
    @Override
    public void run(final SourceContext<SensorReading> ctx) throws Exception {
        final Random rnd = new Random();
        final ScheduledExecutorService exec = Executors.newScheduledThreadPool(2);
        final int idOffset = getRuntimeContext().getIndexOfThisSubtask() * numSensors;
                
        try {
            while (running) {
                long baseTimestamp = System.currentTimeMillis();
                
                // create a variably delayed event from all sensors
                for (int i = 0; i < numSensors; i++) {
                    long shiftInInterval = rnd.nextInt(baseInterval) - baseInterval;
                    
                    double reading = rnd.nextDouble() < fractionOfExceedingReadings ?
                            maxRegularReading + rnd.nextDouble() * maxRegularReading :
                            rnd.nextDouble() * maxRegularReading;

                    String sensorName = String.format("sensor-%04d", i + idOffset);
                    String groupName = String.format("group-%03d", i % numSensorGroups);
                    
                    long delay = rnd.nextInt(maxDelay);
                    
                    final SensorReading event = new SensorReading(groupName, sensorName, 
                            baseTimestamp + shiftInInterval, reading);
                    
                    exec.schedule( () -> {
                        try {
                            ctx.collect(event);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }, delay, TimeUnit.MILLISECONDS);
                }
                
                Thread.sleep(baseInterval);
            }
        }
        finally {
            exec.shutdownNow();
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
