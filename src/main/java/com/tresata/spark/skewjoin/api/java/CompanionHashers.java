package com.tresata.spark.skewjoin.api.java;

public class CompanionHashers {

  public static class CMSHasherInteger extends CMSHasherWrapper<Integer> {
    @Override
    public int hash(int a, int b, int width, Integer x) {
      int unModded = (x * a) + b;
      long modded = (unModded + (unModded >> 32)) & Integer.MAX_VALUE;
      return ((int) modded) % width;
    }
  }

}
