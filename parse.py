import multiprocessing
from multiprocessing import Process, Queue
import xml.etree.cElementTree as etree
import json


class ParseClinVar(object):
  serializer = json
  dest_file = 'solvebio.json'
  iterate_over = 'ClinVarSet'  #top level elem that we will be iterating over

  element_mapping = {  #run et.find() for these elems
    'Title': [{
      'output_path': 'title', 'attr': False, 'clean_func': unicode}],

    'ReferenceClinVarAssertion/ClinVarAccession': [
      {'output_path': 'rcvaccession',
      'attr': 'Acc', 'clean_func': unicode},
      {'output_path': 'rcvaccession_version',
       'attr': 'Version', 'clean_func': int}],

    'ReferenceClinVarAssertion/MeasureSet/Measure/Name/ElementValue': [
      {'output_path': 'prefered_name', 'attr': False, 'clean_func': unicode}],

    'ReferenceClinVarAssertion/MeasureSet/Measure': [
      {'output_path': 'type', 'attr': 'Type', 'clean_func': unicode}],

    'ReferenceClinVarAssertion/ClinicalSignificance/Description': [
      {'output_path': 'clinical_significance', 'attr': False, 'clean_func': unicode}],

  }
  multi_element_mapping = { # run et.iterfind() for these elems
    'ReferenceClinVarAssertion/MeasureSet/Measure/AttributeSet/Attribute': [
      {'output_path': 'hgvs', 'attr': False, 'clean_func': unicode,
       'filter_func': lambda elem: "HGVS" in elem.attrib.get('Type', "")}],

    'ReferenceClinVarAssertion/MeasureSet/Measure/MeasureRelationship/XRef': [
      {'output_path': 'entrez_gene_id', 'attr': 'ID', 'clean_func': unicode,
       'filter_func': lambda elem: elem.attrib.get('DB') == 'Gene'}],

    'ReferenceClinVarAssertion/MeasureSet/Measure/XRef': [
      {'output_path': 'entrez_gene_id', 'attr': 'ID', 'clean_func': lambda id: unicode(id) + "rs",
       'filter_func': lambda elem: elem.attrib.get('DB') == 'Gene'}],
  }

  computed_elements = {
    'rcvaccession_full': lambda doc: "{}.{}".format(
      doc.get('rcvaccession', ""), unicode(doc.get('rcvaccession_version', ""))),

    'uuid': lambda doc: doc.get('rcvaccession_full')
  }

  def __init__(self, filename=None, output=None, num_procs=8):
    self.num_procs = num_procs
    self.filename = filename
    self.dest_file = output or self.dest_file

  def iter_elems(self):
    ''' Iterate over specified elem without loading file into memory '''
    iterator = iter(etree.iterparse(self.filename, events=('start', 'end')))
    event, root_elem = iterator.next()
    for event, elem in iterator:
      if event == 'end' and elem.tag == self.iterate_over:
        yield elem
        root_elem.clear()

  @classmethod
  def get_elem_data(self, elem, output):
    if output['attr']:
      data = elem.attrib.get(output['attr'])
    else:
      data = elem.text
    data = output['clean_func'](data) # clean up data
    return data

  @classmethod
  def to_json(cls, top_elem):
    doc = {}

    for k, v in cls.element_mapping.iteritems():
      elem = top_elem.find(k)
      if elem is not None:
        for output in v:
          data = cls.get_elem_data(elem, output)
          doc[output['output_path']] = data

    for k, v in cls.multi_element_mapping.iteritems():
      for output in v:
        doc[output['output_path']] = []

      for elem in top_elem.iterfind(k):
        for output in v:
          if output['filter_func'](elem):
            data = cls.get_elem_data(elem, output)
            doc[output['output_path']].append(data)

    for k, v in cls.computed_elements.iteritems():
      doc[k] = v(doc)
    return cls.serializer.dumps(doc)


  def run_single(self):
    num_elems = 0
    with open(self.dest_file, "a") as f:
      for elem in self.iter_elems():
        num_elems +=1
        data = self.to_json(elem)
        f.write(data + '\n')
    return num_elems

  def run_multi(self):
    procs = []
    queue = Queue()
    for i in range(self.num_procs):
      proc = Process(target=worker, args=(queue, self.dest_file))
      proc.start()
      procs.append(proc)
    for elem in self.iter_elems():
      if elem is not None:
        queue.put(elem)



def worker(queue, output_file):
  parser = ParseClinVar()
  with open(output_file, "a") as f:
    while True:
      elem = queue.get()
      #print elem
      f.write(parser.to_json(elem) + '\n')













#parser = ParseClinVar('/Users/ian/Downloads/ClinVarFullRelease_2014-08.xml')
#parser.run_single()
