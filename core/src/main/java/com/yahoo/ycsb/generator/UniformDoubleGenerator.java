package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

/**
 * Generates Floats uniform from an interval
 */
public class UniformDoubleGenerator extends NumberGenerator {
  private final double lb, ub;

  public UniformDoubleGenerator(double lb, double ub) {
    this.lb = lb;
    this.ub = ub;
  }

  @Override
  public Double nextValue() {
    double ret = Utils.random().nextDouble() * (ub - lb) + lb;
    setLastValue(ret);

    return ret;

  }

  @Override
  public double mean() {
    return ((lb + ub)) / 2.0;
  }
}
