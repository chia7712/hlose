package com.chia7712.hlose.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class ParseLog {
  public static void main(String[] args) throws IOException {
    String leftKey = "org.apache.hadoop.hbase.regionserver.StoreScanner  - [CHIA] ";
    String rightKey = ":";
    File f = new File("/home/chia7712/com.chia7712.hlose.tool.TestLoadKeyFromFile.html");
    try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
      String line = null;
      Set<String> keys = new TreeSet<>();
      long match = 0;
      boolean nextScan = false;
      long count = 0;
      long noneCount = 0;
      long flushCount = 0;
      while ((line = reader.readLine()) != null) {
        if (line.contains("[[FLUSH]]")) {
          flushCount++;
        }
        if (line.contains("[[NONE]]")) {
          noneCount++;
        }
//        final int leftIndex = line.indexOf(leftKey);
//        if (leftIndex < 0) {
//          if (line.contains("[CHIA]")) {
//            System.out.println("line:" + line);
//          }
//          continue;
//        }
//        ++count;
//        final int rightIndex = line.indexOf(rightKey, leftKey.length() + leftIndex);
//        final String key = line.substring(leftKey.length() + leftIndex, rightIndex);
//        final String context = line.substring(rightIndex + 1);
//
//        if (nextScan) {
//          if (keys.contains(key)) {
//            ++match;
//          } else {
//            System.out.println(key + ":" + context);
//          }
//        } else {
//          if (keys.contains(key)) {
//            nextScan = true;
//          } else {
//            keys.add(key);
//          }
//        }
      }
      System.out.println("noneCount:" + noneCount + ", flushCount:" + flushCount);
    }
  }
}
