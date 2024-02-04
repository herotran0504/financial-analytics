package com.miu.finance.producer;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class FinanceAPIClient {

    public static String fetchHistoricalData(String symbol, long startTimestamp, long endTimestamp) throws Exception {
        String baseUrl = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v3/get-historical-data";

        String query = String.format("symbol=%s&region=US&period1=%d&period2=%d&frequency=%s&filter=%s", symbol, startTimestamp, endTimestamp, "1d", "history");

        URI uri = URI.create(baseUrl + '?' + query);

        HttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(uri);
        httpGet.setHeader("x-rapidapi-host", "apidojo-yahoo-finance-v1.p.rapidapi.com");
        httpGet.setHeader("x-rapidapi-key", "a5783ca9b2mshd1b9b38036dca4fp16143bjsna41c0ff20533");
        System.out.println("Fetching data of symbol: " + symbol);

        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();

        if (entity != null) {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(entity.getContent()))) {
                StringBuilder result = new StringBuilder();
                String line;
                while ((line = in.readLine()) != null) {
                    result.append(line);
                }
                System.out.println("result::" + result);
                return result.toString();
            }
        }

        return null;
    }
}
