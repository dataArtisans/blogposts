# Event Time Windows Example

This example shows how to use event time windows in [Apache Flink](https://flink.apache.org)

The simple example creates a synthetic stream of sensor readings from a set of sensors. The readings are created roughly every 5 seconds, but have a have varying delay, which causes them to arrive out of order and delayed.

The stream is analyzed in three ways simultaneously, printing the results to stdout:

  - **Low-latency event-at-a-time filter**: Readings above a certain cause immediately alerts (latency of milliseconds afterevent  arrival).

  - **Processing Time Windows**: Getting the maximum of all sensors in a group of sensors every 5 seconds (based on event arrival).

  - **Event Time Windows**: Computing average reading per sensor per minute (sliding) based on event timestamp. The windows have a delay of a few seconds, because they are only computed once the event time watermark signals that all relevant events have been received.


The main class of this applications are [Application.java](src/main/java/com/dataartisans/blogpost/eventtime/java/Application.java) (Java), and [Application.scala](src/main/scala/com/dataartisans/blogpost/eventtime/scala/Application.scala) (Scala) respectively.



