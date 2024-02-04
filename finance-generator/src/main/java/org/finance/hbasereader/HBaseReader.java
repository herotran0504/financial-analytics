package org.finance.hbasereader;

import com.miu.finance.model.FinanceData;
import com.miu.finance.model.FinanceDataHBase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class HBaseReader {

    private static final Pattern PATTERN = Pattern.compile("\\d");

    public static List<FinanceDataHBase> getFinanceAnalysis(ResultScanner scanner) {
        List<FinanceDataHBase> dataHBaseList = new ArrayList<>();
        for (Result result : scanner) {
            dataHBaseList.add(parseResultToPrice(result));
        }

        return dataHBaseList;
    }

    private static FinanceDataHBase parseResultToPrice(Result result) {
        FinanceData.Price price = new FinanceData.Price();
        FinanceDataHBase hBaseData = new FinanceDataHBase();

        String rowKey = Bytes.toString(result.getRow());
        hBaseData.setKey(normalizeKey(rowKey));
        for (Cell cell : result.listCells()) {
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            switch (qualifier) {
                case "date":
                    price.setDate((long) Double.parseDouble(value));
                    break;
                case "open":
                    price.setOpen(Double.parseDouble(value));
                    break;
                case "high":
                    price.setHigh(Double.parseDouble(value));
                    break;
                case "low":
                    price.setLow(Double.parseDouble(value));
                    break;
                case "close":
                    price.setClose(Double.parseDouble(value));
                    break;
                case "volume":
                    price.setVolume((long) Double.parseDouble(value));
                    break;
                case "adjclose":
                    price.setAdjclose(Double.parseDouble(value));
                    break;
                default:
                    break;
            }
        }
        hBaseData.setPrice(price);
        return hBaseData;
    }

    private static String normalizeKey(String rowKey) {
        return PATTERN.matcher(rowKey).replaceAll("");
    }

}
