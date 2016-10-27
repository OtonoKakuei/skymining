package largespace.business;

import com.beust.jcommander.Parameter;

import java.util.Map;
import java.util.TreeMap;

public final class OptionsOwn {

	// mode. Means what to do.
	// 0 = preprocessing
	// 1 = recommend queries
	// 2 = evaluate
	@Parameter(names = "-MODE")
	public Integer mode = 0;

	// originalTableTF.setText("QRS_STATEMENTS");
	// accessareaTableTF.setText("QRS_STATEMENTS_PP");
	// userSessionTableTF.setText("QRS_USER_SESSIONS");

	@Parameter(names = "-serverAddress")
	public String serverAddress = "marsara.ipd.kit.edu:1521:student";
	@Parameter(names = "-username")
	public String username = "arzamasova";
	@Parameter(names = "-password")
	public String password = "arzamasova";

	@Parameter(names = "-logTable")
	public String logTable = "QRS_STATEMENTS_PP";

	public Map<String, Table> tablesWithKeys = new TreeMap<String, Table>();

}