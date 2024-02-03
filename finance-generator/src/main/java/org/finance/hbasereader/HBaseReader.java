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

			ResultScanner scanner = table.getScanner(scan);

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
}
