package com.robert.kafka.kclient;
public class PickUpKafkaConfig {


    /**
     * 消息系统topicname
     */
    public static String PICKUP_TOPIC_NAME;
    
	public static int PICKUP_PARTITION_NUM ;

    public static String getPICKUP_TOPIC_NAME() {
        return PICKUP_TOPIC_NAME;
    }

    public static void setPICKUP_TOPIC_NAME(String PICKUP_TOPIC_NAME) {
        PickUpKafkaConfig.PICKUP_TOPIC_NAME = PICKUP_TOPIC_NAME;
    }

    public static int getPICKUP_PARTITION_NUM() {
        return PICKUP_PARTITION_NUM;
    }

    public static void setPICKUP_PARTITION_NUM(int PICKUP_PARTITION_NUM) {
        PickUpKafkaConfig.PICKUP_PARTITION_NUM = PICKUP_PARTITION_NUM;
    }
}
