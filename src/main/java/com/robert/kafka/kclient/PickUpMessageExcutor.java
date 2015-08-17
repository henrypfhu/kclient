package com.robert.kafka.kclient;


import org.slf4j.Logger;
import org.springframework.stereotype.Component;

import com.robert.kafka.kclient.KafkaConsumerClient.MessageExecutor;

@Component
public class PickUpMessageExcutor  implements MessageExecutor
{
    private final static Logger logger =null;
    
    public void execute(String message) {

//        PRARMLIST
//        1 时间   %   %  到秒
//        2 商品名称
//        3 订单号
//        4 提货码
//        5 商家门店地址
//        6 活动有效期


        if (message == null) {
            logger.info("the message from kafka is null , ignore it");
            return;
        }

        logger.info("the message from kafka is :" + message);

        try {
          //  service.opt(message);
        } catch (Exception e) {
            logger.error("exception occurred when dealing message.", e);
        }

    }

}
