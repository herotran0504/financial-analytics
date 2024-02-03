package com.miu.finance.model;

import java.util.List;

public class FinanceData {
    private List<Price> prices;
    private boolean isPending;
    private long firstTradeDate;
    private String id;
    private TimeZone timeZone;
    private List<EventData> eventsData;

    public List<Price> getPrices() {
        return prices;
    }

    public void setPrices(List<Price> prices) {
        this.prices = prices;
    }

    public boolean isPending() {
        return isPending;
    }

    public void setPending(boolean pending) {
        isPending = pending;
    }

    public long getFirstTradeDate() {
        return firstTradeDate;
    }

    public void setFirstTradeDate(long firstTradeDate) {
        this.firstTradeDate = firstTradeDate;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public List<EventData> getEventsData() {
        return eventsData;
    }

    public void setEventsData(List<EventData> eventsData) {
        this.eventsData = eventsData;
    }

    public static class Price {
        private long date;
        private double open;
        private double high;
        private double low;
        private double close;
        private long volume;
        private double adjclose;

        public long getDate() {
            return date;
        }

        public void setDate(long date) {
            this.date = date;
        }

        public double getOpen() {
            return open;
        }

        public void setOpen(double open) {
            this.open = open;
        }

        public double getHigh() {
            return high;
        }

        public void setHigh(double high) {
            this.high = high;
        }

        public double getLow() {
            return low;
        }

        public void setLow(double low) {
            this.low = low;
        }

        public double getClose() {
            return close;
        }

        public void setClose(double close) {
            this.close = close;
        }

        public long getVolume() {
            return volume;
        }

        public void setVolume(long volume) {
            this.volume = volume;
        }

        public double getAdjclose() {
            return adjclose;
        }

        public void setAdjclose(double adjclose) {
            this.adjclose = adjclose;
        }

        @Override
        public String toString() {
            return "Price{" + "date=" + date + ", open=" + open + ", high=" + high + ", low=" + low + ", close=" + close + ", volume=" + volume + ", adjclose=" + adjclose + '}';
        }
    }

    public static class TimeZone {
        private int gmtOffset;

        public int getGmtOffset() {
            return gmtOffset;
        }

        public void setGmtOffset(int gmtOffset) {
            this.gmtOffset = gmtOffset;
        }

    }

    public static class EventData {
        private double amount;
        private long date;
        private String type;
        private double data;

        public double getAmount() {
            return amount;
        }

        public void setAmount(double amount) {
            this.amount = amount;
        }

        public double getData() {
            return data;
        }

        public void setData(double data) {
            this.data = data;
        }

        public long getDate() {
            return date;
        }

        public void setDate(long date) {
            this.date = date;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }
}
