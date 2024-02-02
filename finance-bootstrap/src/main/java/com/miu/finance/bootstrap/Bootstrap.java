package com.miu.finance.bootstrap;

import com.miu.finance.consumer.FinanceConsumer;
import com.miu.finance.producer.FinanceProducer;

public class Bootstrap {

    public static void main(String[] args) {
        FinanceProducer.start();
        FinanceConsumer.start();
    }

}
