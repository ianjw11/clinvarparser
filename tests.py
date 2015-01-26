import unittest
from parse import ParseClinVar
import os


def test_num_records(source, output):
  p = ParseClinVar(filename=source, output=output)
  num_records = int(p.run_single())
  return num_records

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

class NumRecordsTest(unittest.TestCase):
  source='ClinVarFullRelease_2014-08.xml'
  output = 'test_json.json'
  correct_len = 126198

  def setUp(self):
    try:
      os.remove(self.output)
    except:
      pass

  def test(self):
    num_records = test_num_records(self.source, self.output)
    self.assertEqual(num_records, self.correct_len)
    num_lines = file_len(self.output)
    self.assertEqual(num_lines, self.correct_len)

