#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Copied then modified from the wordcount.py example of Beam Python SDK.

"""A naive simulation of the variant merging pipeline."""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class FilterOrKeyDoFn(beam.DoFn):
  """Parses each line of input text and filters those starting with '##'."""

  def __init__(self):
    super(FilterOrKeyDoFn, self).__init__()

  def process(self, element):
    """Returns stripped version of element if it does not start with '##'.

    Args:
      element: the element being processed

    Returns:
      The processed element.
    """
    text_line = element.strip()
    if text_line.startswith('##'):
      return
    parts = text_line.split()
    if len(parts) < 5:
      return
    key_str = ':'.join([parts[0], parts[1], parts[2], parts[3], parts[4]])
    yield (key_str, text_line)


class MergeDoFn(beam.DoFn):
  """Adds all 'calls' to the same variant."""

  def __init__(self):
    super(MergeDoFn, self).__init__()

  def process(self, (key, lines)):
    if not lines:
      return
    output = ''
    for line in lines:
      if not output:
        output = [line]
      else:
        words = line.split()
        # This section is added for making this a CPU intensive DoFn.
        s = 1
        for i in range(10000):
          for w in words:
            s = (s * (i + len(w)) + 1) % 1000
        # End of dummy CPU intensive part.
        if words:
          output.append(words[len(words)-1])
          output.append(str(s))
    yield '\t'.join(output)


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  # Read the text file[pattern] into a PCollection.
  lines = p | 'read' >> ReadFromText(known_args.input)

  # Merges lines with the same "key".
  merged_lines = (
      lines
      | 'filter_or_key' >> beam.ParDo(FilterOrKeyDoFn())
      | 'group' >> beam.GroupByKey()
      | 'merge' >> beam.ParDo(MergeDoFn()))

  # Write the output using a "Write" transform that has side effects.
  merged_lines | 'write' >> WriteToText(known_args.output)

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
