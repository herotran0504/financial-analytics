package org.finance.hbasereader;

import com.miu.finance.model.FinanceData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.miu.finance.utils.FinanceUtils.TABLE_NAME;

<<<<<<< Updated upstream
	public List<FinanceDataHBase> GetFiannceAnalysis() throws IOException
	{
//		List<FinanceData.Price> priceList=new ArrayList<FinanceData.Price>();
		List<FinanceDataHBase> dataHBaseList=new ArrayList<FinanceDataHBase>();
	
		try (Connection connection = ConnectionFactory.createConnection(this.hbaseConfig))
		{
			Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
			Scan scan = new Scan();
			scan.setCacheBlocks(false);
			scan.setCaching(10000);
			scan.setMaxVersions(10);
=======
public class HBaseReader {

    private final Configuration hbaseConfig;
>>>>>>> Stashed changes

    public HBaseReader() {
        this.hbaseConfig = HBaseConfiguration.create();
    }

<<<<<<< Updated upstream
			for (Result result : scanner) {
			//	FinanceDataHBase hBaseData = new FinanceDataHBase();
				System.out.println("price: " + result);
				FinanceDataHBase hBaseData = parseResultToPrice(result);
				if (hBaseData != null) {
					dataHBaseList.add(hBaseData);
				}
			}
		}
		return dataHBaseList;
	}
	private FinanceDataHBase parseResultToPrice(Result result) {

		FinanceData.Price price = new FinanceData.Price();
		FinanceDataHBase hBaseData = new FinanceDataHBase();
		
	    // Extracting the row key from the Result object
	    String rowKey = Bytes.toString(result.getRow());
	    hBaseData.setKey(rowKey); // Setting the row key on the Price object
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
				price.setVolume((long)Double.parseDouble(value));
				break;
			case "adjclose":
				price.setAdjclose(Double.parseDouble(value));
				break;
			default:
				// Handle any other columns you may have
				break;
			}
		}
		hBaseData.setPrice(price);
		return hBaseData;
	}
=======
    private static FinanceData.Price parseResultToPrice(Result result) {
        FinanceData.Price price = new FinanceData.Price();
        for (Cell cell : result.listCells()) {
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            switch (qualifier) {
                case "date":
                    price.setDate(Long.parseLong(value));
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
                    price.setVolume(Long.parseLong(value));
                    break;
                case "adjclose":
                    price.setAdjclose(Double.parseDouble(value));
                    break;
                default:
                    break;
            }
        }
        return price;
    }

    public List<FinanceData.Price> getFinanceAnalysis() throws IOException {
        List<FinanceData.Price> priceList = new ArrayList<>();

        try (Connection connection = ConnectionFactory.createConnection(this.hbaseConfig)) {
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Scan scan = new Scan();
            scan.setCacheBlocks(false);
            scan.setCaching(10000);
            scan.setMaxVersions(10);

            ResultScanner scanner = table.getScanner(scan);

            for (Result result : scanner) {
                System.out.println("price: " + result);
                FinanceData.Price price = parseResultToPrice(result);
                priceList.add(price);
            }
        }
        return priceList;
    }
>>>>>>> Stashed changes
}
