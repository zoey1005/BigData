from mrjob.job import MRJob

# object_oriented VS mapper.py, reducer.py(process_oriented)
class MRWordFrequencyCount(MRJob):
    def mapper(self, _, line):
        yield 'chars', len(line)
        yield 'words', len(line.split())
        yield 'lines', 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__=='__main__':
    MRWordFrequencyCount.run()
