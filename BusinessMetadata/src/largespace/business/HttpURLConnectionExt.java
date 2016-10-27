package largespace.business;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import aima.core.util.datastructure.Pair;
import largespace.clustering.Column;
import largespace.clustering.Column.GlobalColumnType;
import largespace.clustering.DictionaryField;
import largespace.clustering.DistributedFieldWithEmissions;
import largespace.clustering.ValueState;

public class HttpURLConnectionExt {

	private String baseUrl = "http://skyserver.sdss.org/dr12/en/tools/search/x_sql.aspx";
	private final String USER_AGENT = "Mozilla/5.0";
	private final int correctresponseCode = 200;

	public String prepareQuery(String cmd) {
		return cmd.replaceAll(" ", "%20");
	}

	public String getParams(String cmd, String format) {
		return "?cmd=" + cmd + "&format=" + format;
	}

	public Table sendGetTableCount(String tableName) throws Exception {
		Table t = new Table();
		t.name = tableName;
		String prepareCmd = prepareQuery("select count(1) from " + tableName);

		String url = baseUrl + getParams(prepareCmd, "csv");

		URL obj = new URL(url);
		try {
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			con.setConnectTimeout(5000);
			// con.setReadTimeout(20000);

			// optional default is GET
			con.setRequestMethod("GET");

			// add request header
			con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();

			if (responseCode == correctresponseCode) {
				BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();

				int iLineCount = 0;
				while ((inputLine = in.readLine()) != null) {
					if (iLineCount == 2) {
						response.append(inputLine);
					}
					iLineCount++;
				}
				in.close();

				// print result
				// System.out.println(response.toString());
				try {
					int foo = Integer.parseInt(response.toString());
					t.count = foo;
				} catch (Exception ex) {
					System.out.println(ex);
				}
			}
		} catch (java.net.SocketTimeoutException e) {
			System.out.println("java.net.SocketTimeoutException for column: " + t.name);

			// return false;
		} catch (java.io.IOException e) {
			System.out.println("java.io.IOException for column: " + t.name);

			// return false;
		}
		return t;
	}

	// return the List of results.
	// we need to know the Pair <Integer, Object> where Integer value is
	// tableId, Object is an objId - the key from that table.
	// f.e., we have table T, let's table T have TableId = 1;
	// In the internal database table T have the key column "id". All ids from
	// table T are unique.
	// suchwise we need to know only ids to identify the object from table T
	// Thus, pair <tableId, objId (= id in our case)> show us which tuples the
	// query "touched"
	public List<Pair<Table, Object>> sendGetResultFromQuery(RowInfo ri, HashMap<String, Table> tables)
			throws Exception {
		List<Pair<Table, Object>> res = new ArrayList<Pair<Table, Object>>();

		String prepareCmd = prepareQuery(ri.statement);
		String url = baseUrl + getParams(prepareCmd, "csv");

		URL obj = new URL(url);
		try {
			HttpURLConnection.setFollowRedirects(false);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			con.setInstanceFollowRedirects(true);
			con.setConnectTimeout(5000);
			// we shouldn't set timeout here because of we need to know at any
			// case
			con.setReadTimeout(50000);
			// optional default is GET
			con.setRequestMethod("GET");

			// add request header
			con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();

			if (responseCode == correctresponseCode) {
				BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
				String inputLine;

				int iLineCount = 0;

				// TODO: need to find out what field we query
				// store result
				Boolean isOk = false;
				HashMap<Integer, List<Table>> map = new HashMap<Integer, List<Table>>();
				while ((inputLine = in.readLine()) != null) {
					if (iLineCount < 2) {
						if (iLineCount == 0) {
							if (inputLine.equals("#Table1")) {
								// it is Ok, we got some result
								isOk = true;
							}
						}
						if ((iLineCount == 1) && isOk) {
							// parse line with columns' names
							String[] columns = inputLine.split(",");
							// now we have the names of columns in the result
							// set.
							// we need to find the match with key columns for
							// tables in the from clause
							map = getIndexesOfTheKeyColumns(columns, tables);
						}
					} else {
						if (isOk) {
							String[] values = inputLine.split(",");
							for (Integer item : map.keySet()) {
								Object objToStore = values[item];
								List<Table> tablesToStore = map.get(item);
								for (Table t : tablesToStore) {
									Pair<Table, Object> pair = new Pair<Table, Object>(t, objToStore);
									res.add(pair);
								}
							}
						}

					}
					iLineCount++;
				}

				in.close();
			}
		} catch (java.net.SocketTimeoutException e) {
			System.out.println("java.net.SocketTimeoutException for seq: " + ri.seq);
			// return false;
		} catch (java.io.IOException e) {
			System.out.println("java.io.IOException for seq: " + ri.seq);
			// return false;
		}

		return res;
	}

	public HashMap<Integer, List<Table>> getIndexesOfTheKeyColumns(String[] columns, HashMap<String, Table> tables) {
		HashMap<Integer, List<Table>> res = new HashMap<Integer, List<Table>>();
		for (Table t : tables.values()) {
			int i = 0;
			for (String column : columns) {
				if (column.toLowerCase().equals(t.keyColumn.name.toLowerCase())) {
					// we found match for the key column, store this
					if (res.containsKey(i)) {
						List<Table> currList = res.get(i);
						currList.add(t);
						res.put(i, currList);
					} else {
						List<Table> currList = new ArrayList<Table>();
						currList.add(t);
						res.put(i, currList);
					}
					break;
				}
				i++;
			}
		}
		return res;

	}

	public long sendGetDistinctColumnCount(String tableName, String columnName, Options opt) throws Exception {
		long distColumnCount = -1;

		String prepareCmd = prepareQuery("select count(distinct " + columnName + ") from " + tableName);
		String url = baseUrl + getParams(prepareCmd, "csv");

		URL obj = new URL(url);
		try {
			HttpURLConnection.setFollowRedirects(false);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			con.setConnectTimeout(5000);
			// we shouldn't set timeout here because of we need to know at any
			// case
			con.setReadTimeout(50000);
			// optional default is GET
			con.setRequestMethod("GET");

			// add request header
			con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();

			if (responseCode == correctresponseCode) {
				BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();

				int iLineCount = 0;
				while ((inputLine = in.readLine()) != null) {
					if (iLineCount == 2) {
						response.append(inputLine);
					}
					iLineCount++;
				}
				in.close();

				try {
					String respString = response.toString();
					if (respString.equals("")) {
						System.out.println(
								"Didn't have distinct value for column " + columnName + "in table" + tableName);
					} else {
						long foo = Long.parseLong(respString);
						distColumnCount = foo;
					}
				} catch (Exception ex) {
					System.out.println(ex);
				}
			}
		} catch (java.net.SocketTimeoutException e) {
			System.out.println("java.net.SocketTimeoutException for column: " + tableName + "." + columnName);
			// return false;
		} catch (java.io.IOException e) {
			System.out.println("java.io.IOException for column: " + tableName + "." + columnName);
			// return false;
		}
		System.out.println("opt.COLUMNS_DISTRIBUTION.size() =  " + opt.COLUMNS_DISTRIBUTION.size());
		return distColumnCount;
	}

	public List<Long> sendGetMinMaxColumnFromId(String tableName, String columnName) throws Exception {
		List<Long> minMaxList = new ArrayList<Long>();

		String prepareCmd = prepareQuery("select min(" + columnName + "), max(" + columnName + ") from " + tableName);
		String url = baseUrl + getParams(prepareCmd, "csv");

		URL obj = new URL(url);
		try {
			HttpURLConnection.setFollowRedirects(false);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			con.setConnectTimeout(5000);
			// we shouldn't set timeout here because of we need to know at any
			// case
			con.setReadTimeout(50000);
			// optional default is GET
			con.setRequestMethod("GET");

			// add request header
			con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();

			if (responseCode == correctresponseCode) {
				BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();

				int iLineCount = 0;
				while ((inputLine = in.readLine()) != null) {
					if (iLineCount == 2) {
						response.append(inputLine);
					}
					iLineCount++;
				}
				in.close();

				try {
					String[] valueProperty = response.toString().split(",");
					long minValue = Long.parseLong(valueProperty[0]);
					long maxValue = Long.parseLong(valueProperty[1]);
					minMaxList.add(minValue);
					minMaxList.add(maxValue);
				} catch (Exception ex) {
					System.out.println(ex);
				}
			}
		} catch (java.net.SocketTimeoutException e) {
			System.out.println("java.net.SocketTimeoutException for column: " + tableName + "." + columnName);
			// return false;
		} catch (java.io.IOException e) {
			System.out.println("java.io.IOException for column: " + tableName + "." + columnName);
			// return false;
		}
		return minMaxList;
	}

	public List<Double> sendGetMinMaxColumnFromDistrField(String tableName, String columnName) throws Exception {
		List<Double> minMaxList = new ArrayList<Double>();

		String prepareCmd = prepareQuery("select min(" + columnName + "), max(" + columnName + ") from " + tableName);
		String url = baseUrl + getParams(prepareCmd, "csv");

		URL obj = new URL(url);
		try {
			HttpURLConnection.setFollowRedirects(false);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			con.setConnectTimeout(5000);
			// we shouldn't set timeout here because of we need to know at any
			// case
			con.setReadTimeout(50000);
			// optional default is GET
			con.setRequestMethod("GET");

			// add request header
			con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();

			if (responseCode == correctresponseCode) {
				BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();

				int iLineCount = 0;
				while ((inputLine = in.readLine()) != null) {
					if (iLineCount == 2) {
						response.append(inputLine);
					}
					iLineCount++;
				}
				in.close();

				try {
					String[] valueProperty = response.toString().split(",");
					Double minValue = Double.parseDouble(valueProperty[0]);
					Double maxValue = Double.parseDouble(valueProperty[1]);
					minMaxList.add(minValue);
					minMaxList.add(maxValue);
				} catch (Exception ex) {
					System.out.println(ex);
				}
			}
		} catch (java.net.SocketTimeoutException e) {
			System.out.println("java.net.SocketTimeoutException for column: " + tableName + "." + columnName);
			// return false;
		} catch (java.io.IOException e) {
			System.out.println("java.io.IOException for column: " + tableName + "." + columnName);
			// return false;
		}
		return minMaxList;
	}

	public Column sendGetColumnDistribution(String tableName, String columnName, Options opt) throws Exception {
		Column c = new Column(tableName + '.' + columnName);
		c.globalColumnType = GlobalColumnType.DictionaryField;
		c.distribution = new DictionaryField();
		String prepareCmd = prepareQuery("select " + columnName + ", count(*) from " + tableName + " group by "
				+ columnName + " order by " + columnName);
		String url = baseUrl + getParams(prepareCmd, "csv");

		URL obj = new URL(url);
		try {
			HttpURLConnection.setFollowRedirects(false);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			con.setConnectTimeout(5000);
			con.setReadTimeout(50000);
			// optional default is GET
			con.setRequestMethod("GET");

			// add request header
			con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();

			if (responseCode == correctresponseCode) {
				BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();

				int iLineCount = 0;
				long accumulateCount = 0;

				boolean somethingwrong = false;

				while ((inputLine = in.readLine()) != null) {
					if (iLineCount < 2) {
					} else {
						String[] valueProperty = inputLine.split(",");
						String columnValue = valueProperty[0].toLowerCase();
						Long valueCount = Long.parseLong(valueProperty[1]);
						accumulateCount = accumulateCount + valueCount;
						try {
							((DictionaryField) c.distribution).addValue(columnValue,
									new ValueState(valueCount, accumulateCount, columnValue));
						} catch (Exception e) {

						}

						response.append(inputLine + ";");
					}
					iLineCount++;
				}

				if (iLineCount <= 2)
					somethingwrong = true;
				if (somethingwrong) {
					c.somethingWrong = somethingwrong;
				}
				in.close();
			}
		} catch (java.net.SocketTimeoutException e) {
			System.out.println("java.net.SocketTimeoutException for column: " + c.name);

			writePenaltyColumn(opt, c);

		} catch (java.io.IOException e) {
			System.out.println("java.io.IOException for column: " + c.name);

			writePenaltyColumn(opt, c);
		}
		System.out.println("opt.COLUMNS_DISTRIBUTION.size() =  " + opt.COLUMNS_DISTRIBUTION.size());
		return c;
	}

	public void writePenaltyColumn(Options opt, Column c) throws Exception {
		Column c1 = opt.PENALTY_COLUMNS_DISTRIBUTION.get(c.name);
		if (c1 == null) {

			opt.PENALTY_COLUMNS_DISTRIBUTION.put(c.name, c);

			String columnspenOutputFile = opt.FILE_CLMN_OUTPUT.replaceAll(".csv", "_penalty.csv");
			File f = new File(columnspenOutputFile);
			if (f.exists() && !f.isDirectory()) {
				// do something
			} else {
				f.createNewFile();
				BufferedWriter writer = new BufferedWriter(new FileWriter(new File(columnspenOutputFile)));
				writer.close();
			}
			BufferedWriter writer = new BufferedWriter(new FileWriter(columnspenOutputFile, true));

			writer.write(c.name);
			writer.write(";");
			writer.newLine();
			writer.close();
		}
	}

	public Column sendGetColumnEmissions(String tableName, String columnName, Options opt, Table t) throws Exception {
		Column c = new Column(tableName + '.' + columnName);
		c.distribution = new DistributedFieldWithEmissions();
		Long minValueToBeEmission = Math.round(t.count * opt.MIN_PART_TO_BE_EMISSION);
		if (minValueToBeEmission == 0)
			minValueToBeEmission++;

		String prepareCmd = prepareQuery("select " + columnName + ", count(*) from " + tableName + " group by "
				+ columnName + " having count(*) > " + minValueToBeEmission.toString() + " order by " + columnName);
		String url = baseUrl + getParams(prepareCmd, "csv");

		URL obj = new URL(url);
		try {
			HttpURLConnection.setFollowRedirects(false);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			con.setConnectTimeout(5000);
			// con.setReadTimeout(50000);
			// optional default is GET
			con.setRequestMethod("GET");

			// add request header
			con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();

			if (responseCode == correctresponseCode) {
				BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();

				int iLineCount = 0;
				long accumulateCount = 0;

				boolean somethingwrong = false;

				while ((inputLine = in.readLine()) != null) {
					if (iLineCount < 2) {
					} else {
						String[] valueProperty = inputLine.split(",");
						Object columnValue = valueProperty[0];
						Long valueCount = Long.parseLong(valueProperty[1]);
						accumulateCount = accumulateCount + valueCount;
						c.globalColumnType = GlobalColumnType.DistributedFieldWithEmissions;
						((DistributedFieldWithEmissions) c.distribution).addValue(columnValue,
								new ValueState(valueCount, accumulateCount, columnValue));

						response.append(inputLine + ";");
					}
					iLineCount++;
				}

				if (iLineCount <= 2)
					somethingwrong = true;
				if (somethingwrong) {
					c.somethingWrong = somethingwrong;
				}
				in.close();
			}
		} catch (java.net.SocketTimeoutException e) {
			System.out.println("java.net.SocketTimeoutException for column: " + c.name);

		} catch (java.io.IOException e) {
			System.out.println("java.io.IOException for column: " + c.name);

		}
		System.out.println("opt.COLUMNS_DISTRIBUTION.size() =  " + opt.COLUMNS_DISTRIBUTION.size());
		return c;
	}

	public Column sendGetColumnWithoutEmissions(String tableName, String columnName, Options opt, Column cEm)
			throws Exception {
		Column c = new Column(tableName + '.' + columnName);
		c.distribution = new DistributedFieldWithEmissions();
		String inClause = "(";
		for (ValueState vs : ((DistributedFieldWithEmissions) cEm.distribution).values.values()) {
			inClause = inClause + vs.value.toString() + ",";
		}
		inClause = inClause + ")";
		inClause = inClause.replace(",)", ")");

		String prepareCmd = prepareQuery("select min(" + columnName + "), max(" + columnName + ") from " + tableName
				+ " where " + columnName + " not in " + inClause);
		String url = baseUrl + getParams(prepareCmd, "csv");

		URL obj = new URL(url);
		try {
			HttpURLConnection.setFollowRedirects(false);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			con.setConnectTimeout(5000);
			// con.setReadTimeout(5000);
			// optional default is GET
			con.setRequestMethod("GET");

			// add request header
			con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();

			if (responseCode == correctresponseCode) {
				BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();

				int iLineCount = 0;
				while ((inputLine = in.readLine()) != null) {
					if (iLineCount == 2) {
						response.append(inputLine);
					}
					iLineCount++;
				}
				in.close();

				try {
					String[] valueProperty = response.toString().split(",");
					double minValue = Double.parseDouble(valueProperty[0]);
					double maxValue = Double.parseDouble(valueProperty[1]);
					((DistributedFieldWithEmissions) c.distribution).minValue = minValue;
					((DistributedFieldWithEmissions) c.distribution).maxValue = maxValue;

				} catch (Exception ex) {
					System.out.println(ex);
				}
			}
		} catch (java.net.SocketTimeoutException e) {
			System.out.println("java.net.SocketTimeoutException for column: " + c.name);

		} catch (java.io.IOException e) {
			System.out.println("java.io.IOException for column: " + c.name);

		}
		System.out.println("opt.COLUMNS_DISTRIBUTION.size() =  " + opt.COLUMNS_DISTRIBUTION.size());
		return c;
	}

	// HTTP GET request
	public void sendGet() throws Exception {

		String url = "http://skyserver.sdss.org/dr12/en/tools/search/x_sql.aspx?cmd=select%20top%202%20ra,dec%20from%20star&format=csv";

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		// optional default is GET
		con.setRequestMethod("GET");

		// add request header
		con.setRequestProperty("User-Agent", USER_AGENT);

//		int responseCode = con.getResponseCode();
		// System.out.println("\nSending 'GET' request to URL : " + url);
		// System.out.println("Response Code : " + responseCode);

		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		// print result
		System.out.println(response.toString());

	}

	// HTTP POST request
	public void sendPost(String cmd) throws Exception {

		String url = "http://skyserver.sdss3.org/public/en/tools/search/x_sql.aspx";

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		// add request header
		con.setRequestMethod("POST");
		con.setRequestProperty("User-Agent", USER_AGENT);
		con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
		String urlParameters = "cmd=" + cmd + "&format=csv";

		// Send post request
		con.setDoOutput(true);
		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
		wr.writeBytes(urlParameters);
		wr.flush();
		wr.close();

		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'POST' request to URL : " + url);
		System.out.println("Post parameters : " + urlParameters);
		System.out.println("Response Code : " + responseCode);

		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		// print result
		System.out.println(response.toString());

	}
}
