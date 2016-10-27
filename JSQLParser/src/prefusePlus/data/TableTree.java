package prefusePlus.data;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import prefuse.data.Schema;
import prefuse.data.Table;
import prefuse.data.Tree;
import prefusePlus.data.column.ColumnMetadataPlus;

/**
 * Convert a column * row table into a tree structure for TreeMap visualization.
 * Algorithm. Here is the basic idea:<p/>
 *      1. Specify a column, named after "label", which should be ordinal variables,
 *         e.g. String;<p/>
 *      2. Collect all unique items in the specified column, which is N. And their
 *         corresponding frequency and percentage;<p/>
 *      3. Create a Root node with ID=0, Content="Root" + "100%", Parent=0;<p/>
 *      4. Create a set of Parent nodes with ID=auto increase, Content=unique lable +
 *         corresponding percentage, Parent = 0.<p/>
 *      5. For detailed tree view, create a copy of the original table, and assign
 *         each row, as a kid, to these Parent nodes according to its value in the
 *         "label" column. ID = auto increased, Content = null, and Parent = the
 *         row number of the row in the parent table.<p/>
 * Node table is like<p/>
 *      ID      Content         Parent<p/>
 *      0       Root+100%       -1<p/>
 *      1       Item1 + 90.00%  0<p/>
 *      ...     ...             0<p/>
 *      p       null            r1<p/>
 *      ...     ...             ...<p/>
 *<p/>
 * Edge table<p/>
 *      SourceKey       TargetKey<p/>
 *      0               1<p/>
 *      0               2<p/>
 *      ...             ...<p/>
 *      1               74<p/>
 *      1               89<p/>
 *      ...             ...<p/>
 *      2               10000<p/>
 *      ...             ...<p/>
 *<p/>
 * 
 * This data structure is generic enough for other applications.<p/><p/>
 * March 8th, 2011: Back to OOP concept, this class should be a derived tree, not
 *                  a data converter.<p/>
 * March 8th, version 1 ready.<p/>
 *
 * @author jzhang<p/>
 */
public class TableTree {

    /* String names */
    public static final String ID   = "id",
                         CONTENT    = "content",
                         PARENT     = "parent",
                         SOURCEKEY  = "sourcekey",
                         TARGETKEY  = "targetkey";

    /* Label for building up a tree */
    private String z_label;

    /* Local data structure for unique items and their frequencies */
    private MapPair[] z_freq_table;

    /* table schema of these two tables*/
    protected final Schema nodeSchema = new Schema();
    {
        nodeSchema.addColumn(ID, int.class);
        nodeSchema.addColumn(CONTENT, String.class);
        nodeSchema.addColumn(PARENT, int.class);
    }
    protected final Schema edgeSchema = new Schema();
    {
        edgeSchema.addColumn(SOURCEKEY, int.class);
        edgeSchema.addColumn(TARGETKEY, int.class);
    }

    /* Two return tables, one for nodes, and one for edges*/
    private Table nodes = nodeSchema.instantiate(),
                  edges = edgeSchema.instantiate();

    /* Source table reference, used for retrieve original data rows.*/
    private Table z_backTable;

    private Tree z_tableTree = null;

    private int rowCount,
                itemCount;

    /**
     * Construct a TableTree data structure, with the back table and specified column
     * name for label.<p/>
     * @param t - back table<P/>
     * @param label - name of column for creating tree structure<P/>
     *
     * NOTE: constructor has too much codes. May need to separate to several methods.<p/>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
	public TableTree(Table t, String label){
                
        // set the column name to what user specified, and check if it is a String class.
        z_label = label;
        Schema ts = t.getSchema();
        Class label_type = ts.getColumnType(z_label);
        if (label_type != String.class){
System.err.println("Can not do this on an interval variable");
            return;
        }

        //get the unique items and their frequencies fromt the source table.
        z_backTable = t;

        ColumnMetadataPlus cmdp = new ColumnMetadataPlus(z_backTable, z_label);
        Map<String, ArrayList> itemPair = cmdp.getUniqueStringListMap();

        z_freq_table = new MapPair[itemPair.size()];

        int i = 0;
        MapPair pairs;

        Iterator it = itemPair.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, ArrayList> pair = (Map.Entry)it.next();
            pairs = new MapPair(pair.getKey(), pair.getValue());
            z_freq_table[i] = pairs;
            i++;
        }
        Arrays.sort(z_freq_table, new MapPairComparator());

        //convert the item + itemlist array to build the Nodes table
        //add the root node
        addNode("Root 100%", -1);

        //add parent nodes
        rowCount = z_backTable.getRowCount();
        itemCount = z_freq_table.length;

        for (int j=0;j<itemCount;j++){

            DecimalFormat f = new DecimalFormat("#.##");
            float perc = ((float) z_freq_table[j].getList().size()/
                            (float) rowCount) * 100;   //calculate percent

            z_freq_table[j].setContent(z_freq_table[j].getContent() + " (" + f.format(perc) + "%)");
            addNode(z_freq_table[j].getContent(),0);
        }

        //add edge between root and parents
        for (int j=0;j<itemCount;j++){
            addEdge(0, j+1);
        }

        //add nodes from the parent's list and build edges at the same time.
        /**
         * the number from a MapPair's list is its original row number
         * "1+itemCount+n" is the new row number
         */
        int kidCount = 0;
        for (int j=0;j<itemCount;j++){
            ArrayList itemlist = z_freq_table[j].getList();
            for (int n=0;n<itemlist.size();n++){
                //get two numbers
                int original_rownumber = (Integer) itemlist.get(n);
                int new_rownumber = 1+itemCount+kidCount;

                //add a new node row
                addNode(z_freq_table[j].getContent(), original_rownumber);
                //add a new edge
                addEdge(1+j, new_rownumber);
                kidCount++;
            }
        }

        //-- . Create a tree
        z_tableTree = new Tree(nodes, edges, SOURCEKEY, TARGETKEY);
    }

    /**
     * Add a row to nodes table with contents and parent. ID is auto increased.
     * @param content
     * @param parent
     */
    private void addNode(String content, int parent){
        int row = nodes.addRow();
        nodes.setInt(row, ID, row);
        nodes.setString(row, CONTENT, content);
        nodes.setInt(row, PARENT, parent);
    }

    /**
     * Add a row to edge table with source and target keys.
     * @param source
     * @param target
     */
    private void addEdge(int source, int target){
        int row = edges.addRow();
        edges.setInt(row, SOURCEKEY, source);
        edges.setInt(row, TARGETKEY, target);
    }

    //-- A set of public method for getting info of these tree
    /**
     * Get the total number of rows in the back table.<p/>
     * @return an int of the number of rows.<p/>
     */
    public int getBackTableRowCount(){
        return this.rowCount;
    }

    /**
     * Get the number of unique item in this column.<p/>
     * @return an int of the number of unique item<p/>
     */
    public int getItemRowCount(){
        return this.rowCount;
    }

    /**
     * Get the create Table tree.<p/>
     * @return a Tree derived from the back table
     */
    public Tree getTableTree(){
        return z_tableTree;
    }

    /**
     * Get the back table.<p/>
     * @return a Table<p/>
     */
    public Table getBackTable(){
        return z_backTable;
    }


    /**
     * A private comparator to sort the MapPair object according to the object's
     * itemlist fields.<p/>
     */
    @SuppressWarnings("rawtypes")
	private class MapPairComparator implements Comparator{

        public int compare(Object o1, Object o2) {
            MapPair mp1 = (MapPair) o1;
            MapPair mp2 = (MapPair) o2;

            int mp1f = mp1.getList().size();
            int mp2f = mp2.getList().size();

            if (mp1f > mp2f){
                return -1;
            } else {
                if (mp1f < mp2f){
                    return 1;
                } else
                    return 0;
            }
        }

    }

    /**
     * An inner class to process ColumnMetadataPlus' return. It is used to sort
     * the returns and build up a tree.<p/>
     *
     */
    private class MapPair{

        String content;
        ArrayList<Integer> itemlist;

        @SuppressWarnings({ "rawtypes", "unchecked" })
		MapPair(String content, ArrayList list){
            this.content = content;
            this.itemlist = list;
        }

        public void setContent(String content){
            this.content = content;
        }

        public String getContent(){
            return this.content;
        }

        @SuppressWarnings({ "rawtypes", "unchecked", "unused" })
		public void setList(ArrayList list){
            this.itemlist = list;
        }

        @SuppressWarnings("rawtypes")
		public ArrayList getList(){
            return this.itemlist;
        }
    }

}
