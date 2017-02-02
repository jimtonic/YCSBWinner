package com.yahoo.ycsb.generator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestUniformDoubleGenerator {

  @Test
  public void testPositiveLowerBound() throws Exception {
    Double value;

    UniformDoubleGenerator gen = new UniformDoubleGenerator(0.0, 1.0);
    for (int i = 0; i < 100; i++) {
      value = gen.nextValue();
      assertTrue(value+" is not >= 0.0", 0.0 <= value);
      assertTrue(value+" is not <= 1.0", 1.0 >= value);
    }

    gen = new UniformDoubleGenerator(1.1, 99.9);
    for (int i = 0; i < 100; i++) {
      value = gen.nextValue();
      assertTrue(value+" is not >= 0.0", 1.1 <= value);
      assertTrue(value+" is not <= 1.0", 99.9 >= value);
    }
  }

  @Test
  public void testNegativeLowerBound() throws Exception {
    Double value;

    UniformDoubleGenerator gen = new UniformDoubleGenerator(-1.0, 0.0);
    for (int i = 0; i < 100; i++) {
      value = gen.nextValue();
      assertTrue(value+" is not >= -1.0", -1.0 <= value);
      assertTrue(value+" is not <= 0.0", 0.0 >= value);
    }

    gen = new UniformDoubleGenerator(-99.9, -1.1);
    for (int i = 0; i < 100; i++) {
      value = gen.nextValue();
      assertTrue(value + " is not >= -99.9", -99.9 <= value);
      assertTrue(value + " is not <= -1.1", -1.1 >= value);
    }
  }

  @Test
  public void testZeroCrossedBounds() throws Exception {
    Double value;

    UniformDoubleGenerator gen = new UniformDoubleGenerator(-100.0, 100.0);
    for (int i = 0; i < 100; i++) {
      value = gen.nextValue();
      assertTrue(value+" is not >= -100.0", -100.0 <= value);
      assertTrue(value+" is not <= 100.0", 100.0 >= value);
    }
  }

  @Test
  public void testMean() throws Exception {
    UniformDoubleGenerator gen = new UniformDoubleGenerator(-100.0, 100.0);
    Double mean = gen.mean();
    assertEquals(0.0, mean, 0.000001);

    gen = new UniformDoubleGenerator(1.1, 99.9);
    mean = gen.mean();
    assertEquals(50.5, mean, 0.000001);
  }
}
