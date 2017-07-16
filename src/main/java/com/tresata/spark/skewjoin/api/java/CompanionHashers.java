package com.tresata.spark.skewjoin.api.java;

public class CompanionHashers {

  public static class CMSHasherLong extends CMSHasherWrapper<Long> {
    @Override
    public int hash(int a, int b, int width, Long x) {
      long unModded = (x * a) + b;
      // Apparently a super fast way of computing x mod 2^p-1
      // See page 149 of http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
      // after Proposition 7.
      long modded = (unModded + (unModded >> 32)) & Integer.MAX_VALUE;
      return ((int) modded) % width;
    }
  }

  public static class CMSHasherInteger extends CMSHasherWrapper<Integer> {
    @Override
    public int hash(int a, int b, int width, Integer x) {
      int unModded = (x * a) + b;
      long modded = (unModded + (unModded >> 32)) & Integer.MAX_VALUE;
      return ((int) modded) % width;
    }
  }

}
