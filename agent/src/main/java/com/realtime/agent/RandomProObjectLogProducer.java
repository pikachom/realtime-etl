/**
 * Copyright @TmaxBI, 2020
 */
package com.realtime.agent;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class RandomProObjectLogProducer {
  final static String BRACKET_FORMAT = "[%s] ";
  final static SimpleDateFormat DT_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  final static String THREAD_FORMAT = "Thread : %d";
  final static String[] LOG_LEVEL = {"DEBUG", "INFO", "FINE", "SEVERE", "ERROR"};
  final static int LOG_LEVEL_MAX_INDEX = 4;
  final static String[] SAMPLE_LOG = {
          "[CALL][GUID-hyperdata][proobject.schedule.ScheduleAgentService->hyperdata.web-service.MonDataFile]",
          "[ServiceEventHandler][REQUEST][GUID-hyperdata][GDV-0] Service is requested - ServiceFullName - hyperdata.web-service.MonTempFile",
          "[ServiceWorkerThreadPool] Service hyperdata.web-service.MonTempFile@2,016,836,003 is allocated thread to ProObject Service Worker[hyperdata.web-service-1]",
          "[GUID-hyperdata]<<hyperdata.web-service.MonTempFile>> is ready for execution : Method - service, WaitObject - com.tmax.proobject.engine.waitobject.WaitObject@3c7e935d",
          "[ServiceEventHandler][REQUEST][GUID-hyperdata][GDV-0] successing find meta data - hyperdata.web-service.MonParameter",
          "[EventChain][GDV-0] GDV Usage is increased (Current Usage : 3)",
          "[GUID-hyperdata]<<hyperdata.web-service.MonParameter>> is ready for execution : Method - service, WaitObject - com.tmax.proobject.engine.waitobject.WaitObject@55dbecdc",
          "[DBSession] session closed",
          "[GUID-hyperdata]<<proobject.schedule.ScheduleAgentService>> is done!",
          "[ServiceEventHandler][REQUEST][GUID-hyperdata][GDV-0] Service is requested - ServiceFullName - hyperdata.web-service.CollectInstance",
          "[GUID-hyperdata][SessionContainer] Getting Non-XA PairDataSource HD7-mon from application hyperdata",
          "[ServiceEventHandler][REQUEST][GUID-hyperdata][GDV-0][COMPLEX] Scheduling ComplexService hyperdata.web-service.MonSysStat ... ",
          "[ServiceEventHandler][REQUEST] Service Name \"MonInstance\" is formatted as \"MonInstance\" according to naming type NATIVE"
  };
  final static int SAMPLE_LOG_MAX_INDEX = 12;

  public static void main(String[] args) {
    long lines = Long.parseLong(args[0]);
    long interval = Long.parseLong(args[1]);
    long iteration = Long.parseLong(args[2]);

    // Set up Java properties
    Properties props = new Properties();
    // This should point to at least one broker. Some communication
    // will occur to find the controller. Adding more brokers will
    // help in case of host failure or broker failure.
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        "localhost:9092");
    // Enable a few useful properties for this example. Use of these
    // settings will depend on your particular use case.
    props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    // Required properties to process records
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    try {
      for (long i = 0; i < iteration; i++) {
        String key = Long.toString(i);
        String msg = "";
        for (long line = 0; line < lines; line++) {
          msg += String.format(BRACKET_FORMAT, DT_FORMAT.format(new Date()));
          msg += String.format(BRACKET_FORMAT, String.format(THREAD_FORMAT, (int) Math.round(Math.random() * 100)));
          msg += String.format(BRACKET_FORMAT, LOG_LEVEL[(int) Math.round(Math.random() * LOG_LEVEL_MAX_INDEX)]);
          msg += SAMPLE_LOG[(int) Math.round(Math.random() * SAMPLE_LOG_MAX_INDEX)];
          msg += "\n";
        }
        ProducerRecord<String, String> data = new ProducerRecord<String, String>("test-topic-1", key, msg);
        producer.send(data);
        Thread.sleep(interval);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      producer.close();
    }
  }
}
