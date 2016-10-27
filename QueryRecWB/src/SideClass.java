import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import accessarea.AccessArea;
import accessarea.AccessAreaExtraction;
import largespace.business.DatabaseInteraction;
import largespace.business.HttpURLConnectionExt;
import largespace.business.Loader;
import largespace.business.Options;
import largespace.business.OptionsOwn;
import largespace.business.RowInfo;
import largespace.business.Table;
import largespace.clustering.Predicate;
import largespace.clustering.QueriesComparision;
import largespace.clustering.Query;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.statement.select.FromItem;

public class SideClass {

	public HashMap<String, Table> GetTablesWithKeysFromTheFromItemsOfStatement(List<FromItem> fromItems,
			OptionsOwn opt) {
		HashMap<String, Table> res = new HashMap<String, Table>();
		for (FromItem fi : fromItems) {
			String fromItem = fi.toString();
			if (!res.containsKey(fromItem)) {
				Table t = opt.TABLESWITHKEYS.get(fromItem.toLowerCase());
				res.put(t.Name, t);
			}
		}
		return res;
	}
}
