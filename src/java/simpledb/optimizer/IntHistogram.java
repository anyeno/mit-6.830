package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;   // 桶的数量
    private double width;
    private int [] height;
    private int min;
    private int max;
    private int ntups;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // TODO: some code goes here
        this.buckets = buckets;
        if(max - min > buckets)
            this.width = (double)(max - min + 1) / buckets;
        else this.width = 1;
        this.height = new int[buckets];
        this.min = min;
        this.max = max;
        this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // TODO: some code goes here
        int n_bucket = (int) ((v - min) / width);
        if(n_bucket < 0 || n_bucket >= buckets) {
            System.out.println("width == " + width);
            System.out.println("min == "+min);
            System.out.println("v == " + v);
        }
        height[n_bucket] ++;
        ntups ++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */

    public double estimateSelectivity(Predicate.Op op, int v) {

        // TODO: some code goes here

        if(op == Predicate.Op.EQUALS) {
            return estimateSelectivityEquals(v);
        } else if(op == Predicate.Op.GREATER_THAN) {
            return estimateSelectivityGreater(v);
        } else if(op == Predicate.Op.LESS_THAN) {
            return estimateSelectivityLesser(v);
        } else if(op == Predicate.Op.NOT_EQUALS) {
            return 1.0 - estimateSelectivityEquals(v);
        } else if(op == Predicate.Op.GREATER_THAN_OR_EQ) {
            return estimateSelectivityEquals(v) + estimateSelectivityGreater(v);
        } else if(op == Predicate.Op.LESS_THAN_OR_EQ) {
            return estimateSelectivityLesser(v) + estimateSelectivityEquals(v);
        }

        return -1.0;
    }

    public double estimateSelectivityEquals(int v) {
        int n_bucket = (int) ((v - min) / width);  //第几个桶，从0开始
        if(n_bucket < 0 || n_bucket >= buckets) {
            return 0;
        }
        return (double)(height[n_bucket] / width) / ntups;
    }
    public double estimateSelectivityGreater(int v) {
        int n_bucket = (int) ((v - min) / width);
        if(n_bucket < 0) {
            return 1.0;
        } else if (n_bucket >= buckets) {
            return 0.0;
        }
        int right_ntups = 0;
        for (int i = n_bucket + 1; i < height.length; i++) {
            right_ntups += height[i];
        }
        double res_p2 = (double)right_ntups / ntups;
        //当前桶内 = v的数量
        int this_bucket_greater_than_v = (int) (min - 1 + (n_bucket + 1)* width - v);
        double res_p1 = (double)this_bucket_greater_than_v * (height[n_bucket] / width) / ntups;
        double res = res_p1 + res_p2;
        return res;
    }
    public double estimateSelectivityLesser(int v) {
        int n_bucket = (int) ((v - min) / width);
        if(n_bucket < 0) {
            return 0.0;
        } else if (n_bucket >= buckets) {
            return 1.0;
        }
        int left_ntups = 0;
        for (int i = 0; i < n_bucket; i++) {
            left_ntups += height[i];
        }
        double res_p2 = (double)left_ntups / ntups;
        //当前桶内 < v的数量
        int this_bucket_lesser_than_v = (int) (v - (min + (int)n_bucket * width - 1) - 1);
        double res_p1 = (double)this_bucket_lesser_than_v * (height[n_bucket] / width) / ntups;
        return res_p1 + res_p2;
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        // TODO: some code goes here
        return "IntHistogram{" +
                "buckets=" + buckets +
                ", width=" + width +
                ", height=" + Arrays.toString(height) +
                ", min=" + min +
                ", max=" + max +
                ", ntups=" + ntups +
                '}';
    }
}
