package com.miu.finance.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class FinanceUtils {

    public static final String TABLE_NAME = "finance_historical_data_table";

    public static long getTimeStamp(LocalDate date) {
        return date.atStartOfDay().toEpochSecond(ZoneOffset.UTC);
    }

    public static long getTimeStamp(LocalDateTime dateTime) {
        return dateTime.toEpochSecond(ZoneOffset.UTC);
    }

}
