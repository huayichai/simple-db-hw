package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;


    private int _tableid;
    private int _ioCostPerPage;
    private TupleDesc td;
    public HeapFile dbFile;
    private HashMap<Integer, int[]> minmaxMap;
    private HashMap<Integer, Object> histogramMap; // key -> 列号
    private int nums; // tuple的数量

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        
        _tableid = tableid;
        _ioCostPerPage = ioCostPerPage;
        Transaction t = new Transaction();
        dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(_tableid);
        DbFileIterator it = dbFile.iterator(t.getId());
        td = dbFile.getTupleDesc();
        minmaxMap = new HashMap<>();
        histogramMap = new HashMap<>();
        nums = 0;

        try {
            // 初始化两个map
            if (!it.hasNext()) {
                return;
            }
            Tuple tuple = it.next();
            for (int i = 0; i < td.numFields(); i++) {
                if (td.getFieldType(i) == Type.INT_TYPE) {
                    int value = ((IntField) tuple.getField(i)).getValue();
                    minmaxMap.put(i, new int[] {value, value});
                }
            }
            it.rewind();

            // 计算int列的最大最小值
            while (it.hasNext()) {
                tuple = it.next();
                nums++;
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        int[] minmax = minmaxMap.get(i);
                        int min_t = minmax[0];
                        int max_t = minmax[1];
                        int value = ((IntField) tuple.getField(i)).getValue();
                        if (value < min_t) {
                            minmax[0] = value;
                        } else if (value > max_t) {
                            minmax[1] = value;
                        }
                        minmaxMap.put(i, minmax);
                    }
                }
            }
            it.rewind();

            // 构造histogram
            for (int i = 0; i < td.numFields(); i++) {
                if (td.getFieldType(i) == Type.INT_TYPE) {
                    int[] minmax = minmaxMap.get(i);
                    IntHistogram histogram = new IntHistogram(NUM_HIST_BINS, minmax[0], minmax[1]);
                    histogramMap.put(i, histogram);
                } else if (td.getFieldType(i) == Type.STRING_TYPE) {
                    StringHistogram histogram = new StringHistogram(NUM_HIST_BINS);
                    histogramMap.put(i, histogram);
                }
            }

            // 向histogram中添加数据
            while (it.hasNext()) {
                tuple = it.next();
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        IntHistogram histogram = (IntHistogram) histogramMap.get(i);
                        histogram.addValue(((IntField) tuple.getField(i)).getValue());
                        histogramMap.put(i, histogram);
                    } else if (td.getFieldType(i) == Type.STRING_TYPE) {
                        StringHistogram histogram = (StringHistogram) histogramMap.get(i);
                        histogram.addValue(((StringField) tuple.getField(i)).getValue());
                        histogramMap.put(i, histogram);
                    }
                }
            }
            it.rewind();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return dbFile.numPages() * IOCOSTPERPAGE;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) Math.ceil(totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (constant.getType() == Type.INT_TYPE) {
            int value = ((IntField)constant).getValue();
            IntHistogram histogram = (IntHistogram) histogramMap.get(field);
            return histogram.estimateSelectivity(op, value);
        } else {
            String value = ((StringField)constant).getValue();
            StringHistogram histogram = (StringHistogram)histogramMap.get(field);
            return histogram.estimateSelectivity(op, value);
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return nums;
    }

}
