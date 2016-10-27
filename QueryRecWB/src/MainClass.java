import com.beust.jcommander.JCommander;

import largespace.business.OptionsOwn;

public class MainClass {
	public static void main(String[] args) throws Exception {
		OptionsOwn opt = new OptionsOwn();
		QueryRec rec = new QueryRec(opt);
		new JCommander(opt, args);

		switch (opt.mode) {
		// preprocessing step store the result of the query to our internal
		// database
		// before the preprocessing step we need to find out which tables we
		// have; which columns they contain; which of them could be identifiers
		// for an object?
		// just a remainder: to compare 2 queries In WB paradigm we need to
		// compare the results of the queries and find common objects.
		// the amount of common objects identify the measure of similarity for 2
		// queries
		// suchwise, first we will get tables with columns (in pre-preprocessing
		// step)
		// after that we create an internal database to store the results of the
		// queries
		// on preprocessing step we fill this internal database

		case 0:
			rec.Preprocess(opt);
			break;
		case 1:
			rec.Recommend(opt);
			break;
		case 2:
			rec.Evaluate(opt);
			break;
		}

	}
}
