package largespace.business;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import largespace.clustering.Cluster;
import largespace.clustering.Column;
import largespace.clustering.Column.GlobalColumnType;
import largespace.clustering.DictionaryField;
import largespace.clustering.DistributedField;
import largespace.clustering.DistributedFieldWithEmissions;
import largespace.clustering.FromCluster;
import largespace.clustering.Identificator;
import largespace.clustering.Predicate;
import largespace.clustering.Query;
import largespace.clustering.ValueState;

public final class Loader {
	private Loader() {
	}

	public static void preprocess(String inputFile, String outputFile) throws IOException {
		Scanner scanner = new Scanner(new FileInputStream(inputFile));
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputFile)));
		Pattern regex = Pattern.compile("\"[^\"]+\"");
		scanner.nextLine();
		while (scanner.hasNext()) {
			String s = scanner.nextLine().replace("\"\"", "");
			Matcher citedMatcher = regex.matcher(s);
			boolean tablesFound = citedMatcher.find();
			int index;
			if (tablesFound && citedMatcher.start() == 0) {
				String[] tables = Regex.commaRegex.split(s.substring(citedMatcher.start() + 1, citedMatcher.end() - 1));
				Arrays.sort(tables);
				writer.write(Utils.join(tables, ", "));
				index = citedMatcher.end();
			} else {
				index = s.indexOf(",");
				String input = s.substring(0, index);
				if (!input.equals("\"\"")) {
					writer.write(input);
				}
			}
			writer.write(";");
			if (tablesFound && (citedMatcher.start() > 0 || citedMatcher.find())) {
				String[] predicates = Regex.andRegex
						.split(s.substring(citedMatcher.start() + 1, citedMatcher.end() - 1));
				for (int i = 0; i < predicates.length; i++) {
					if (predicates[i].startsWith("(") && predicates[i].endsWith(")")) {
						String[] clauses = Regex.orRegex.split(predicates[i].substring(1, predicates[i].length() - 1));
						Arrays.sort(clauses);
						predicates[i] = Utils.join(clauses, " OR ");
					}
				}
				Arrays.sort(predicates);
				writer.write(Utils.join(predicates, " AND "));
				index = citedMatcher.end();
			} else {
				int nextIndex = s.indexOf(",", index + 1);
				String input = s.substring(index + 1, nextIndex);
				if (!input.equals("\"\"")) {
					// TODO WTF?!
				}
				writer.write(input);
				index = nextIndex;
			}
			writer.write(";");
			try {
				long seq = Long.parseLong(s.substring(index + 29));
				writer.write(Long.toString(seq));
			} catch (Exception ex) {
				writer.write("0");
				System.out.println("Error processing " + s);
				System.out.println(s.substring(index + 29));
			}
			writer.newLine();
		}
		scanner.close();
		writer.close();
	}

	public static List<String> listFilesForFolder(final File folder) {
		List<String> files = new ArrayList<String>();
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {

			} else {
				String fileName = fileEntry.getName();
				String[] strList = fileName.split("_");
				if ((strList[0].equals("sample")) || (strList[0].equals("sample.csv")))
					files.add(fileName);
			}
		}
		return files;
	}

	public static List<String> fromClustersQueriesFiles(Options opt) {
		List<String> files = new ArrayList<String>();
		final File folder = new File(opt.FOLDER_C_OUTPUT);
		files = listFilesForFolder(folder);
		return files;
	}

	public static void readFromClause(Query q, Options opt) throws Exception {
		List<String> fromTables = q.fromTables;

		for (String tName : fromTables) {
			Table t = null;
			if (!opt.TABLESWITHCOUNT.containsKey(tName)) {
				HttpURLConnectionExt urlcon = new HttpURLConnectionExt();
				t = urlcon.sendGetTableCount(tName);
				// t = new Table();
				// t.Name = tName;
				opt.TABLESWITHCOUNT.put(tName, t);
				writeTables(opt);
			}
			t = opt.TABLESWITHCOUNT.get(tName);
		}
		for (String tName : fromTables) {
			Table t = opt.TABLESWITHCOUNT.get(tName);
			for (String tNameLinks : fromTables) {
				if (!t.links.contains(tNameLinks)) {
					t.links.add(tNameLinks);
				}
			}
		}
	}

	public static void Postprocess(Options opt) throws Exception {
		for (Table t : opt.TABLESWITHCOUNT.values()) {
			FromCluster resCl = null;
			for (FromCluster fromCl : opt.fromClusters) {
				if (fromCl.fromTables.contains(t.name)) {
					// such a table exists in opt.TABLESWITHCOUNT
					resCl = fromCl;
				}
			}
			for (String link : t.links) {
				for (FromCluster fromCl : opt.fromClusters) {
					if (fromCl.fromTables.contains(link)) {
						// such a table exists in opt.TABLESWITHCOUNT
						resCl = fromCl;
					}
				}
			}
			if (resCl == null) {
				resCl = new FromCluster();
				resCl.fromTables.add(t.name);
				opt.fromClusters.add(resCl);
			}

			for (String link : t.links) {

				if (!resCl.fromTables.contains(link))
					resCl.fromTables.add(link);
			}

		}
	}

	public static Boolean readWhereClause(List<List<Predicate>> wherePart, Options opt, Long lineNumber,
			Boolean isTrash) {
		for (List<Predicate> listPredicates : wherePart) {
			for (Predicate predicate : listPredicates) {
				try {
					String columnName = predicate.table + "." + predicate.column;
					HttpURLConnectionExt urlcon = new HttpURLConnectionExt();

					Table t = null;
					if (!opt.TABLESWITHCOUNT.containsKey(predicate.table)) {
						t = urlcon.sendGetTableCount(predicate.table);
						opt.TABLESWITHCOUNT.put(predicate.table, t);
						writeTables(opt);
					} else
						t = opt.TABLESWITHCOUNT.get(predicate.table);
					if (opt.TABLESWITHCOUNT.get(predicate.table).count > 0) {
						String value = predicate.value;
						Column c = opt.PENALTY_COLUMNS_DISTRIBUTION.get(columnName);
						if (c != null) {
							isTrash = true;
							return isTrash;
						} else {
							c = opt.COLUMNS_DISTRIBUTION.get(columnName);
							if (c != null) {
								if (c.globalColumnType == GlobalColumnType.DictionaryField)
									((DictionaryField) opt.COLUMNS_DISTRIBUTION.get(columnName).distribution)
											.addValue(value);
							} else {

								long distinctValues = urlcon.sendGetDistinctColumnCount(predicate.table,
										predicate.column, opt);
								if (distinctValues != -1) {
									c = new Column(columnName);
									if (t.count == distinctValues) {
										// it's identificator field, save as
										// identificator
										c.globalColumnType = GlobalColumnType.Identificator;

										List<Long> minMaxvalues = urlcon.sendGetMinMaxColumnFromId(predicate.table,
												predicate.column);
										if (minMaxvalues.size() != 2) {
											System.out.println(
													"Can't get minMaxvalues for " + lineNumber + ": " + lineNumber);
											// it means that this field is not
											// numeric, probably the table isn't
											// big
											if (t.count <= opt.MAX_DISTINCT_VALUES_TO_BE_DICTIONARY_FIELD) {
												c.globalColumnType = GlobalColumnType.DictionaryField;
												Column resCol = urlcon.sendGetColumnDistribution(predicate.table,
														predicate.column, opt);
												c.distribution = resCol.distribution;
											} else {
												urlcon.writePenaltyColumn(opt, c);
											}
										} else
											c.distribution = new Identificator(minMaxvalues.get(0), minMaxvalues.get(1),
													distinctValues);
									} else {
										if (distinctValues <= opt.MAX_DISTINCT_VALUES_TO_BE_DICTIONARY_FIELD) {
											c.globalColumnType = GlobalColumnType.DictionaryField;
											Column resCol = urlcon.sendGetColumnDistribution(predicate.table,
													predicate.column, opt);
											c.distribution = resCol.distribution;
										} else {
											// look for Emissions
											Column resCol = urlcon.sendGetColumnEmissions(predicate.table,
													predicate.column, opt, t);
											if (((DistributedFieldWithEmissions) resCol.distribution).values
													.size() > 0) {
												// it's field with emissions;
												c.globalColumnType = GlobalColumnType.DistributedFieldWithEmissions;
												c.distribution = resCol.distribution;
												Column resCol2 = urlcon.sendGetColumnWithoutEmissions(predicate.table,
														predicate.column, opt, resCol);
												((DistributedFieldWithEmissions) c.distribution).minValue = ((DistributedFieldWithEmissions) resCol2.distribution).minValue;
												((DistributedFieldWithEmissions) c.distribution).maxValue = ((DistributedFieldWithEmissions) resCol2.distribution).maxValue;
											} else {
												List<Double> minMaxvalues2 = urlcon.sendGetMinMaxColumnFromDistrField(
														predicate.table, predicate.column);
												if (minMaxvalues2.size() != 2) {
													System.out.println("Can't get minMaxvalues for " + lineNumber + ": "
															+ lineNumber);
													urlcon.writePenaltyColumn(opt, c);
													// it means that this field
													// is not numeric, probably
													// the table isn't big
													/// c.GlobalColumnType =
													// GlobalColumnType.DictionaryField;
													/// Column resCol =
													// urlcon.sendGetColumnDistribution(predicate.table,
													// predicate.column, opt);
													/// c.Distribution =
													// resCol.Distribution;
												} else {
													c.globalColumnType = GlobalColumnType.DistributedField;
													c.distribution = new DistributedField(minMaxvalues2.get(0),
															minMaxvalues2.get(1));
												}
											}
										}
									}

									opt.COLUMNS_DISTRIBUTION.put(columnName, c);
									writeColumn(opt.FILE_CLMN_OUTPUT, c, opt);
								} else {
									// later
									// TODO: check, whether the column exists
									isTrash = true;
									c = new Column(columnName);
									opt.COLUMNS_DISTRIBUTION.put(columnName, c);
									writeColumn(opt.FILE_CLMN_OUTPUT, c, opt);
								}
								try {

									// urlcon.sendGet();

								} catch (Exception ex) {

									ex.printStackTrace();
									urlcon.writePenaltyColumn(opt, c);
								}
								// find out the distribution of the column

							}
						}
					} else {
						isTrash = true;

					}
				} catch (Exception ex) {
					System.out.println("Exception at" + lineNumber + ": " + ex.toString());
				}
			}
		}
		return isTrash;
	}

	public static Map<String, Column> readColumnsFromInputData(Options opt) throws Exception {
		Long pos = new Long(-1);
		String startPositionPath = opt.FILE_INPUT.replace(".csv", opt.FILE_START_POSITION + ".csv");
		File f = new File(startPositionPath);
		if (f.exists() && !f.isDirectory()) {
			// do something
		} else {
			f.createNewFile();
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(startPositionPath)));
			writer.write("0");
			writer.close();
		}

		Scanner position = new Scanner(
				new FileInputStream(opt.FILE_INPUT.replace(".csv", opt.FILE_START_POSITION + ".csv")));

		while (position.hasNext()) {
			String s = position.nextLine();
			pos = Long.parseLong(s);
		}
		position.close();
		Scanner scanner = new Scanner(new FileInputStream(opt.FILE_INPUT));
		// Map<String, Column> data = new HashMap<String, Column>();
		long lineNumber = 0;

		while ((opt.COLUMNS_DISTRIBUTION.size() < opt.MAX_POINTS) && scanner.hasNext()) {

			boolean isTrash = false;
			++lineNumber;
			if (lineNumber % 10 == 0) {
				System.out.println("Processing query " + Long.toString(lineNumber));
				if (lineNumber % 100 == 0) {
					BufferedWriter writer = new BufferedWriter(new FileWriter(new File(startPositionPath)));
					writer.write(Long.toString(lineNumber));
					writer.close();
				}
			}
			String s = scanner.nextLine();
			if (pos <= lineNumber) {

				try {
					String[] vals = s.split(opt.FIELD_DELIMITER);
					if (vals.length != 3) {
						throw new ParseException("Illegal number of columns!", s);
					}

					Query q = new Query(opt.COLUMNS_DISTRIBUTION.size(), vals[0], vals[1], Long.parseLong(vals[2]));
					List<List<Predicate>> wherePart = q.whereClausesTerms;

					readFromClause(q, opt);
					// for test only
					isTrash = readWhereClause(wherePart, opt, lineNumber, isTrash);

				} catch (ParseException e) {
					System.out.println("Line " + lineNumber + ": " + e.toString());
				}
				if (isTrash) {
					opt.trash++;
					System.out.println("opt.Trash = " + Long.toString(opt.trash) + "s = " + s);
					if (opt.trash % 10 == 0) {
						System.out.println("opt.Trash = " + Long.toString(opt.trash));
					}
				}
			}
		}
		scanner.close();

		Scanner penaltyColumns = new Scanner(
				new FileInputStream(opt.FILE_CLMN_OUTPUT.replaceAll(".csv", "_penalty.csv")));
		while (penaltyColumns.hasNext()) {
			String s = penaltyColumns.nextLine();
			String penaltyColumnname = s;
			Column c = new Column(penaltyColumnname);
			opt.PENALTY_COLUMNS_DISTRIBUTION.put(c.name, c);
		}
		penaltyColumns.close();
		return opt.COLUMNS_DISTRIBUTION;

	}

	public static void calculateQueryCountInEachCluster(Options opt) throws Exception {
		Scanner scanner = new Scanner(new FileInputStream(opt.FILE_INPUT));
		// Map<String, Column> data = new HashMap<String, Column>();
		long lineNumber = 0;

		while ((opt.COLUMNS_DISTRIBUTION.size() < opt.MAX_POINTS) && scanner.hasNext()) {

			boolean isTrash = false;
			++lineNumber;
			if (lineNumber % 10 == 0) {
				System.out.println("Processing query " + Long.toString(lineNumber));
			}
			String s = scanner.nextLine();
			try {
				String[] vals = s.split(opt.FIELD_DELIMITER);
				if (vals.length != 3) {
					throw new ParseException("Illegal number of columns!", s);
				}

				Query q = new Query(opt.COLUMNS_DISTRIBUTION.size(), vals[0], vals[1], Long.parseLong(vals[2]));
				List<List<Predicate>> wherePart = q.whereClausesTerms;
				String t = q.fromTables.get(0);
				FromCluster resCl = null;
				for (FromCluster fromCl : opt.fromClusters) {
					if (fromCl.fromTables.contains(t)) {
						// such a table exists in opt.TABLESWITHCOUNT
						resCl = fromCl;
						resCl.queryCount++;
						resCl.data.add(s);
					}
				}

			} catch (ParseException e) {
				System.out.println("Line " + lineNumber + ": " + e.toString());
			}
			if (isTrash) {
				opt.trash++;
				System.out.println("opt.Trash = " + Long.toString(opt.trash) + "s = " + s);
				if (opt.trash % 10 == 0) {
					System.out.println("opt.Trash = " + Long.toString(opt.trash));
				}
			}
		}
		scanner.close();

	}

	public static List<Query> readInputData(Options opt, String inputFileName) throws FileNotFoundException {
		Scanner scanner = new Scanner(new FileInputStream(inputFileName));
		List<Query> data = new LinkedList<>();
		long lineNumber = 0;

		while ((data.size() < opt.MAX_POINTS) && scanner.hasNext()) {
			++lineNumber;
			if (lineNumber % 10 == 0) {
				System.out.println("Processing query " + Long.toString(lineNumber));
			}
			String s = scanner.nextLine();
			try {
				String[] vals = s.split(opt.FIELD_DELIMITER);
				if (vals.length != 3) {
					throw new ParseException("Illegal number of columns!", s);
				}

				Query q = new Query(data.size(), vals[0], vals[1], Long.parseLong(vals[2]));

				{
					if (opt.TABLES != null) {
						for (String table : q.fromTables) {
							boolean hasTable = false;
							for (String tbl : opt.TABLESWITHCOUNT.keySet()) {
								if (tbl.equalsIgnoreCase(table)) {
									hasTable = true;
									break;
								}
							}
							if (!hasTable) {
								throw new ParseException("Illegal Table!", table);
							}
						}
					}

					// if everything is fine, then include the data
					data.add(q);
					/// corrections
				}
				/// corrections
			} catch (ParseException e) {
				System.out.println("Line " + lineNumber + ": " + e.toString());
			}
		}
		scanner.close();

		return new ArrayList<>(data);
	}

	public static void writeColumn(String columnsOutputFile, Column c, Options opt) throws Exception {
		System.out.println("opt.Trash = " + opt.trash);

		File f = new File(columnsOutputFile);
		if (f.exists() && !f.isDirectory()) {
			// do something
		} else {
			f.createNewFile();
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(columnsOutputFile)));
			writer.close();
		}
		BufferedWriter writer = new BufferedWriter(new FileWriter(columnsOutputFile, true));

		writer.write(c.name);
		writer.write(";");
		if (c.globalColumnType == GlobalColumnType.Identificator) {
			try {
				writer.write("Identificator");
				writer.write(";");
				writer.write(Long.toString(((Identificator) c.distribution).minValue));
				writer.write(";");
				writer.write(Long.toString(((Identificator) c.distribution).maxValue));
				writer.write(";");
			} catch (Exception e) {
				writer.write("Exception, e = " + e.toString());
			}
		}
		if (c.globalColumnType == GlobalColumnType.DistributedField) {
			try {
				writer.write("DistributedField");
				writer.write(";");
				writer.write(Double.toString(((DistributedField) c.distribution).minValue));
				writer.write(";");
				writer.write(Double.toString(((DistributedField) c.distribution).maxValue));
				writer.write(";");
			} catch (Exception e) {
				writer.write("Exception, e = " + e.toString());
			}
		}
		if (c.globalColumnType == GlobalColumnType.DistributedFieldWithEmissions) {
			try {
				writer.write("DistributedFieldWithEmissions");
				writer.write(";");
				writer.write(Double.toString(((DistributedFieldWithEmissions) c.distribution).minValue));
				writer.write(";");
				writer.write(Double.toString(((DistributedFieldWithEmissions) c.distribution).maxValue));
				writer.write(";");
				writer.write("Emissions");
				writer.write(";");
				Map<Object, ValueState> values = ((DistributedFieldWithEmissions) c.distribution).values;
				for (ValueState v : values.values()) {
					try {
						if (v.value == null)
							v.value = "null";
						writer.write(v.value.toString() + " " + v.valuesCount + " " + v.valuesLessOrEqualCount);
						writer.write(";");
					} catch (Exception ex) {
						// System.out.println("ex = " + ex.toString());
					}
				}
			} catch (Exception e) {
				writer.write("Exception, e = " + e.toString());
			}
		}
		if (c.globalColumnType == GlobalColumnType.DictionaryField) {
			try {
				writer.write("DictionaryField");
				writer.write(";");
				writer.write("Values");
				writer.write(";");
				Map<String, ValueState> values = ((DictionaryField) c.distribution).values;
				for (ValueState v : values.values()) {
					try {
						if (v.value != null) {
							writer.write(v.value.toString() + " " + v.valuesCount + " " + v.valuesLessOrEqualCount);
							writer.write(";");
						}
					} catch (Exception ex) {
						// System.out.println("ex = " + ex.toString());
					}
				}
			} catch (Exception e) {
				writer.write("Exception, e = " + e.toString());
			}

		}
		writer.newLine();
		writer.close();

	}

	public static void writeColumns(String columnsOutputFile, Map<String, Column> columns, Options opt)
			throws Exception {
		System.out.println("opt.Trash = " + opt.trash);
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(columnsOutputFile)));
		int cid = 0;
		int clusterId = 0;
		for (Column c : columns.values()) {
			writer.write(c.name);
			writer.write(";");
			if (c.globalColumnType == GlobalColumnType.Identificator) {
				try {
					writer.write("Identificator");
					writer.write(";");
					writer.write(Long.toString(((Identificator) c.distribution).minValue));
					writer.write(";");
					writer.write(Long.toString(((Identificator) c.distribution).maxValue));
					writer.write(";");
				} catch (Exception e) {
					writer.write("Exception, e = " + e.toString());
				}
			}
			if (c.globalColumnType == GlobalColumnType.DistributedField) {
				try {
					writer.write("DistributedField");
					writer.write(";");
					writer.write(Double.toString(((DistributedField) c.distribution).minValue));
					writer.write(";");
					writer.write(Double.toString(((DistributedField) c.distribution).maxValue));
					writer.write(";");
				} catch (Exception e) {
					writer.write("Exception, e = " + e.toString());
				}
			}
			if (c.globalColumnType == GlobalColumnType.DistributedFieldWithEmissions) {
				try {
					writer.write("DistributedFieldWithEmissions");
					writer.write(";");
					writer.write(Double.toString(((DistributedFieldWithEmissions) c.distribution).minValue));
					writer.write(";");
					writer.write(Double.toString(((DistributedFieldWithEmissions) c.distribution).maxValue));
					writer.write(";");
					writer.write("Emissions");
					writer.write(";");
					Map<Object, ValueState> values = ((DistributedFieldWithEmissions) c.distribution).values;
					for (ValueState v : values.values()) {
						try {
							if (v.value == null)
								v.value = "null";
							writer.write(v.value.toString() + " " + v.valuesCount + " " + v.valuesLessOrEqualCount);
							writer.write(";");
						} catch (Exception ex) {
							// System.out.println("ex = " + ex.toString());
						}
					}
				} catch (Exception e) {
					writer.write("Exception, e = " + e.toString());
				}
			}
			if (c.globalColumnType == GlobalColumnType.DictionaryField) {
				try {
					writer.write("DictionaryField");
					writer.write(";");
					writer.write("Values");
					writer.write(";");
					Map<String, ValueState> values = ((DictionaryField) c.distribution).values;
					for (ValueState v : values.values()) {
						try {
							if (v.value != null) {
								writer.write(v.value.toString() + " " + v.valuesCount + " " + v.valuesLessOrEqualCount);
								writer.write(";");
							}
						} catch (Exception ex) {
							// System.out.println("ex = " + ex.toString());
						}
					}
				} catch (Exception e) {
					writer.write("Exception, e = " + e.toString());
				}
			}
			writer.newLine();

		}

		writer.close();

	}

	public static void writeTables(Options opt) throws Exception {

		BufferedWriter writer = new BufferedWriter(
				new FileWriter(new File(opt.FILE_TBL_OUTPUT.replaceAll(".csv", "_clusters.csv"))));
		for (FromCluster fromCluster : opt.fromClusters) {
			writer.write(Long.toString(fromCluster.queryCount));
			writer.write(";");
			for (String fromtable : fromCluster.fromTables) {
				writer.write(fromtable);
				writer.write(";");
			}
			writer.newLine();
		}
		writer.close();

		int i = 0;
		for (FromCluster fromCluster : opt.fromClusters) {
			writer = new BufferedWriter(
					new FileWriter(new File(opt.FOLDER_C_OUTPUT + "\\sample_" + Integer.toString(i) + ".csv")));
			for (String s : fromCluster.data) {
				writer.write(s);
				writer.newLine();
			}
			writer.close();
			i++;
		}
		writer = new BufferedWriter(new FileWriter(new File(opt.FILE_TBL_OUTPUT)));
		int cid = 0;
		int clusterId = 0;
		for (Table t : opt.TABLESWITHCOUNT.values()) {
			writer.write(t.name);
			writer.write(";");
			writer.write(Long.toString(t.count));
			writer.write(";");
			writer.write("Links");
			writer.write(";");
			for (String link : t.links) {
				writer.write(link);
				writer.write(";");
			}
			writer.newLine();
		}

		writer.close();
	}

	public static void readTables(Options opt) throws Exception {
		String file = opt.FILE_TBL_OUTPUT;
		File f = new File(file);
		if (f.exists() && !f.isDirectory()) {
			// do something
		} else {
			f.createNewFile();
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(file)));
			writer.close();
		}
		Scanner scanner = new Scanner(new FileInputStream(opt.FILE_TBL_OUTPUT));
		List<Query> data = new LinkedList<>();
		long lineNumber = 0;

		while ((data.size() < opt.MAX_POINTS) && scanner.hasNext()) {
			++lineNumber;
			if (lineNumber % 10 == 0) {
				System.out.println("Processing table " + Long.toString(lineNumber));
			}
			String s = scanner.nextLine();
			try {
				String[] vals = s.split(opt.FIELD_DELIMITER);
				Table t = new Table(vals);
				opt.TABLESWITHCOUNT.put(t.name, t);

				/// corrections
			} catch (Exception e) {
				System.out.println("Line " + lineNumber + ": " + e.toString());
			}
		}
		scanner.close();
	}

	public static void readColumns(Options opt, String columnsOutputFile) throws Exception {

		String file = columnsOutputFile;
		File f = new File(file);
		if (f.exists() && !f.isDirectory()) {
			// do something
		} else {
			f.createNewFile();
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(file)));
			writer.close();
		}
		Scanner scanner = new Scanner(new FileInputStream(columnsOutputFile));
		List<Query> data = new LinkedList<>();
		long lineNumber = 0;

		while ((data.size() < opt.MAX_POINTS) && scanner.hasNext()) {
			++lineNumber;
			if (lineNumber % 10 == 0) {
				System.out.println("Processing table " + Long.toString(lineNumber));
			}
			String s = scanner.nextLine();
			try {
				String[] vals = s.split(opt.FIELD_DELIMITER);
				Column c = new Column(vals);
				opt.COLUMNS_DISTRIBUTION.put(c.name, c);

				/// corrections
			} catch (Exception e) {
				System.out.println("Line " + lineNumber + ": " + e.toString());
			}
		}
		scanner.close();

		file = columnsOutputFile.replaceAll(".csv", "_penalty.csv");
		f = new File(file);
		if (f.exists() && !f.isDirectory()) {
			// do something
		} else {
			f.createNewFile();
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(file)));
			writer.close();
		}
		scanner = new Scanner(new FileInputStream(file));
		data = new LinkedList<>();
		lineNumber = 0;

		while ((data.size() < opt.MAX_POINTS) && scanner.hasNext()) {
			++lineNumber;
			if (lineNumber % 10 == 0) {
				System.out.println("Processing table " + Long.toString(lineNumber));
			}
			String s = scanner.nextLine();
			try {
				Column c = new Column(s);
				opt.PENALTY_COLUMNS_DISTRIBUTION.put(c.name, c);

				/// corrections
			} catch (Exception e) {
				System.out.println("Line " + lineNumber + ": " + e.toString());
			}
		}
		scanner.close();
	}

	public static void writeQueries(String outputFile, List<Cluster> clusters) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputFile)));
		int cid = 0;
		int clusterId = 0;
		for (Cluster c : clusters) {
			for (Query q : c.points) {
				writer.write(Integer.toString(q.id));
				writer.write(";");
				writer.write(Integer.toString(cid++));
				writer.write(";");
				writer.write(q.getFromString());
				writer.write(";");
				writer.write(q.getWhereString());
				writer.write(";");
				writer.write(Long.toString(q.seq));
				writer.write(";");
				writer.write(Long.toString(clusterId));

				for (int dx : q.d) {
					writer.write(";");
					writer.write(Integer.toString(dx));
				}

				writer.newLine();
			}
			clusterId++;
		}

		writer.close();
	}

	public static Map<String, double[]> readMaxMin(String fileInput) throws FileNotFoundException {
		Scanner scanner = new Scanner(new FileInputStream(fileInput));
		Map<String, double[]> res = new HashMap<>();

		while (scanner.hasNext()) {
			String[] vals = scanner.nextLine().split(";");

			double[] mm = new double[2];
			try {
				mm[0] = Double.parseDouble(vals[1]);
				mm[1] = Double.parseDouble(vals[2]);

				res.put(vals[0], mm);
			} catch (Exception e) {
			}
		}

		scanner.close();
		return res;
	}

	public static Query[] readInputData(String fileInput, String delimiter) throws FileNotFoundException {
		Options opt = new Options();
		opt.FILE_INPUT = fileInput;
		opt.FIELD_DELIMITER = delimiter;
		List<Query> data = readInputData(opt, opt.FILE_INPUT);

		return data.toArray(new Query[data.size()]);
	}
}
