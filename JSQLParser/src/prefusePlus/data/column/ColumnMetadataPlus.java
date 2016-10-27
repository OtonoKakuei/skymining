
package prefusePlus.data.column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import prefuse.data.Table;

/**
 * @author James
 * Nov. 18th, 2010: Extends from prefuse.data.column.ColumnMetadat for other statistics
 *                  calculation
 * NOTE: not extends anymore. Not useful!
 */
public class ColumnMetadataPlus {

    /** variables  for control*/
    protected int[] m_ordinalF;
    protected Map<String, Integer> m_ordinalSFMap;
    @SuppressWarnings("rawtypes")
	protected Map<String, ArrayList> z_ordinalSLMap;

    private Table m_table;
    private String m_field;

    /**
     *
     * @param parent
     * @param column
     */
    public ColumnMetadataPlus(Table parent, String column){
        m_table = parent;
        m_field = column;
        m_ordinalSFMap = null;
        z_ordinalSLMap = null;
    }

    /**
     * Set the column for metadata.
     * @param column name of column for metadata
     */
    public void setColumn(String column){
        m_field = column;
        clearCachedValues();
    }

    public void calculateValues(){
        clearCachedValues();
        getUniqueStringFrequencyMap();
//        getUniqueStringFrequency();
    }

    /**
     * NOTE: better leave this operation to other classes.
     * @return
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
	public int[] getUniqueStringFrequency(){
        
        if (m_ordinalF == null){
            getUniqueStringFrequencyMap();
            m_ordinalF = new int[m_ordinalSFMap.size()];
            int counter = 0;
            Iterator it = m_ordinalSFMap.keySet().iterator();
            while (it.hasNext()){
                Map.Entry<String, Integer> pairs = (Map.Entry)it.next();
                m_ordinalF[counter] = pairs.getValue();
            }
            Arrays.sort(m_ordinalF);
        }
        return m_ordinalF;
    }

    /**
     * NOTE: Support String type only.
     * @return a map contains unique objects and their frequency.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	public Map<String, Integer> getUniqueStringFrequencyMap(){

        if (m_ordinalSFMap == null){

            m_ordinalSFMap = new HashMap();

            //if type of the column is String, proceed
            if ( m_table.getColumnType(m_field) == String.class){
                int size = m_table.getRowCount();
                String s;
                int freq;
                //iterate the column
                for (int i=0;i<size;i++){
                    s = (String) m_table.get(i, m_field);
                    if (m_ordinalSFMap.containsKey(s)){
                        freq = m_ordinalSFMap.get(s);
                        m_ordinalSFMap.put(s, ++freq);
                    } else
                        m_ordinalSFMap.put(s, 1);
                }
            } else throw new UnsupportedOperationException("Not supported non-String type yet.");
        }

        return m_ordinalSFMap;
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "unused" })
	public Map<String, ArrayList> getUniqueStringListMap(){

        if (z_ordinalSLMap == null){

            z_ordinalSLMap = new HashMap();

            //if type of the column is String, proceed
            if ( m_table.getColumnType(m_field) == String.class){
                int size = m_table.getRowCount();
                String s;
                int freq;
                //iterate the column
                for (int i=0;i<size;i++){
                    s = (String) m_table.get(i, m_field);
                    if (z_ordinalSLMap.containsKey(s)){
                        ArrayList itemlist = z_ordinalSLMap.get(s);
                        itemlist.add(i);
                        z_ordinalSLMap.put(s, itemlist);
                    } else{
                        ArrayList itemlist = new ArrayList();
                        itemlist.add(i);
                        z_ordinalSLMap.put(s, itemlist);
                    }
                }
            } else throw new UnsupportedOperationException("Not supported non-String type yet.");
        }

        return z_ordinalSLMap;
    }

    private void clearCachedValues(){
        m_ordinalF = null;
        m_ordinalSFMap = null;
        z_ordinalSLMap = null;
    }

}
