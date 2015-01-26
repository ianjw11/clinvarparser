from parse import ParseClinVar
import argparse



parser = argparse.ArgumentParser(description='transform clinvar from xml to json')
parser.add_argument('--source', dest='source', type=str,
                   help='source xml file')
parser.add_argument('--output', dest='output', type=str,
                   help='output json file')


if __name__ == "__main__":
  args = parser.parse_args()
  p = ParseClinVar(filename=args.source, output=args.output)
  p.run_single()