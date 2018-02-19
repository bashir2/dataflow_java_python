/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * A naive simulation of the Variant Transforms pipeline.
 * Modified from the WordCount example of Beam Java SDK.
 */
public class CallMerger {

  /**
   * This DoFn filters out lines starting with "##" and create a key for others.
   */
  static class FilterOrKeyDoFn extends DoFn<String, KV<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element().startsWith("##")) return;
      // Split the line into words.
      String[] words = c.element().trim().split("\\s+");
      if (words.length < 5) return;
      StringBuilder b = new StringBuilder();
      b.append(words[0]).append(":");
      b.append(words[1]).append(":");
      b.append(words[2]).append(":");
      b.append(words[3]).append(":");
      b.append(words[4]);
      String key = b.toString();

      c.output(KV.of(key, c.element()));
    }
  }

  /**
   * This DoFn merges lines with the same key by taking the first full line and
   * adding the last word of other lines (e.g., simulating merging samples).
   */
  static class MergeDoFn extends DoFn<KV<String, Iterable<String>>, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      boolean first = true;
      StringBuilder b = new StringBuilder();
      for (String line : c.element().getValue()) {
        if (first) {
          b.append(line);
          first = false;
        } else {
          String[] words = line.split("\\s+");
          // This section is added for making this a CPU intensive DoFn.
          int s = 1;
          for (int i = 0; i < 10000; i++) {
            for (String word : words) {
              s = (s * (i + word.length()) + 1) % 1000;
            }
          }
          // End of dummy CPU intensive part.
          if (words.length > 0) {
            b.append("\t").append(words[words.length - 1]).append("\t").append(s);
          }
        }
      }
      c.output(b.toString());
    }
  }

  /**
   * A PTransform that converts a PCollection containing lines of text into a
   * PCollection of merged lines based on line keys.
   */
  public static class MergeCalls extends PTransform<PCollection<String>,
      PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<KV<String, String>> keyLines = lines.apply(
          ParDo.of(new FilterOrKeyDoFn()));

      PCollection<KV<String, Iterable<String>>> groupedLines =
          keyLines.apply(GroupByKey.<String, String>create());

      PCollection<String> mergedLines = groupedLines.apply(
          ParDo.of(new MergeDoFn()));
      return mergedLines;
    }
  }

  /**
   * Options supported by {@link CallMerger}.
   */
  public interface CallMergerOptions extends PipelineOptions {

    /**
     * By default, this example reads from a public dataset containing the text
     * of King Lear. Set this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();
    void setInputFile(String value);

    /**
     * Set this required option to specify where to write the output.
     */
    @Description("Path of the file to write to")
    @Required
    String getOutput();
    void setOutput(String value);
  }

  public static void main(String[] args) {
    CallMergerOptions options = PipelineOptionsFactory.fromArgs(args)
      .withValidation().as(CallMergerOptions.class);
    Pipeline p = Pipeline.create(options);

    p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
     .apply(new MergeCalls())
     .apply("WriteCounts", TextIO.write().to(options.getOutput()));

    p.run().waitUntilFinish();
  }
}
