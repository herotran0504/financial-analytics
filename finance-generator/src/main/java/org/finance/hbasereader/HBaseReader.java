package org.finance.hbasereader;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.miu.finance.model.*;

public class HBaseReader 
{
	private Configuration hbaseConfig;
	private final String TABLE_NAME="finance_historical_data_table";
	public HBaseReader()
	{
		this.hbaseConfig = HBaseConfiguration.create();
	}

	public List<FinanceData.Price> GetFiannceAnalysis() throws IOException
	{
		List<FinanceData.Price> priceList=new ArrayList<FinanceData.Price>();

		try (Connection connection = ConnectionFactory.createConnection(this.hbaseConfig))
		{
			Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
			Scan scan = new Scan();
			scan.setCacheBlocks(false);
			scan.setCaching(10000);
			scan.setMaxVersions(10);

			ResultScanner scanner = table.getScanner(scan);

			for (Result result : scanner) {
				System.out.println("price: " + result);
				FinanceData.Price price = parseResultToPrice(result);
				System.out.println("price: "+ "22222222222");
				System.out.println("price: " + price);
				if (price != null) {
					priceList.add(price);
				}
			}
		}
		return priceList;
	}
	private FinanceData.Price parseResultToPrice(Result result) {
		// Assuming the row key could be the combination of key and date as in your persist method
		FinanceData.Price price = new FinanceData.Price();
		for (Cell cell : result.listCells()) {
			String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
			String value = Bytes.toString(CellUtil.cloneValue(cell));
			System.out.println("qualifier: " + qualifier);
			System.out.println("value: " + value);
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
				// Handle any other columns you may have
				break;
			}
		}
		return price;
	}
}
