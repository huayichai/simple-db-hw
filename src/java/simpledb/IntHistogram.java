package simpledb;

import java.util.ArrayList;

import javafx.scene.control.ButtonType;
import simpledb.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;
    private int _min;
    private int _max;
    private int interval;
    private int ntups;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        _min = min;
        _max = max;
        this.buckets = new int[buckets];
        interval = (int) Math.ceil((1.0 + max - min)/this.buckets.length);
        ntups = 0;
    }

    private int getIndex(int v) {
        // if (v < _min || v > _max) {
        //     throw new IllegalArgumentException("value out of range");
        // }
        if (v == _max) {
            return buckets.length - 1;
        }
        return (int)((v - _min) / interval);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        buckets[getIndex(v)]++;
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int index = getIndex(v);
        int left = interval * index + _min;
        int right = (interval + 1) * index + _min - 1;
        int height;
        switch (op) {
            case EQUALS:
                if (v < _min || v > _max) {
                    return 0.0;
                } else {
                    height = buckets[index];
                    return (height * 1.0 / interval) / ntups;
                }
            case GREATER_THAN:
                if (v < _min) {
                    return 1.0;
                }
                if (v > _max) {
                    return 0.0;
                }
                height = buckets[index];
                double p1 = (height * 1.0 / ntups) * ((right - v) * 1.0 / interval);
                int allInRight = 0;
                for (int i = index + 1; i < buckets.length; i++) {
                    allInRight += buckets[i];
                }
                double p2 = allInRight * 1.0 / ntups;
                return p1 + p2;
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            case LESS_THAN:
                if (v < _min) {
                    return 0.0;
                }
                if (v > _max) {
                    return 1.0;
                }
                height = buckets[index];
                double pp1 = ((v - left) / interval * 1.0) * (height * 1.0 / ntups);
                int allInLeft = 0;
                for (int i = index - 1; i >= 0; i--) {
                    allInLeft += buckets[i];
                }
                double pp2 = allInLeft * 1.0 / ntups;
                return pp1 + pp2;
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return 0.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        int cnt = 0;
        for(int bucket:buckets) cnt += bucket;
        if(cnt ==0) return 0.0;
        return cnt/ntups;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return String.format("IntHistgram(buckets=%d, min=%d, max=%d", buckets.length, _min, _max);
    }
}
