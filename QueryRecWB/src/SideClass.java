import java.util.HashMap;
import java.util.List;
import java.util.Map;

import largespace.business.OptionsOwn;
import largespace.business.Table;
import net.sf.jsqlparser.statement.select.FromItem;

public class SideClass {

	public Map<String, Table> getTablesWithKeysFromTheFromItemsOfStatement(List<FromItem> fromItems,
			OptionsOwn opt) {
		Map<String, Table> res = new HashMap<String, Table>();
		for (FromItem fi : fromItems) {
			String fromItem = fi.toString();
			if (!res.containsKey(fromItem)) {
				Table t = opt.tablesWithKeys.get(fromItem.toLowerCase());
				res.put(t.name, t);
			}
		}
		return res;
	}
}
