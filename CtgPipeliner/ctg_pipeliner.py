from collections import defaultdict
from itertools import zip_longest
from glob import glob
import os
import pandas as pd
import json
import gzip

from .sge_writer import SgeWriter

class CtgPipeline(object): 
    """Automates running CTG"""

    def __init__(
        self,
        fastq_directory=None,
        working_directory=None 
    ):

        if fastq_directory is None: 
            fastq_directory = os.getcwd()

        if working_directory is None: 
            working_directory = os.getcwd()

        self.fastq_directory = fastq_directory
        self.working_directory = working_directory


    def parse_files(
        self, 
        glob_pattern, 
        timepoint_prefix="", 
        delimiter="_", 
        job_index=0, 
        tpt_index=1, 
        rep_index=2, 
        read_index=3,
        datetime=False
    ):

        files = glob(os.path.join(self.fastq_directory, glob_pattern))

        if not files: 
            raise RuntimeError("Did not find any files!")

        # Categorizing files
        grouped_files = defaultdict(lambda : [[], []]) # creating container for paired end reads
        jobs = []
        for fn in files:
            base_filename = os.path.basename(fn.split('.')[0])
            
            arr = base_filename.split(delimiter)
            
            tpt = arr[tpt_index]
            rep = arr[rep_index]

            if arr[read_index] == 'R1': 
                grouped_files[(tpt, rep)][0].append(fn) 
            elif arr[read_index] == 'R2': 
                grouped_files[(tpt, rep)][1].append(fn)
            else: 
                raise RuntimeError("Encountered invalid read identifier (%s)" % arr[read_index])

            jobs.append(arr[job_index])

        # Organizing reads 
        for tup, fns in grouped_files.items(): 
            a = sorted(fns[0]) 
            b = sorted(fns[1])

            for i,j in zip_longest(a,b): 
                if edit_distance(i,j) != 1: 
                    raise RuntimeError("Sorting did not yield perfect pairing of files")

            grouped_files[tup] = [a,b]


        jobs = set(jobs) 
        if len(jobs) != 1: 
            raise RuntimeError("Did not find only one job!")

        self.grouped_files = dict(grouped_files)
        self.job = list(jobs)[0]
        self.files = set(files)
        self.timepoint_prefix = timepoint_prefix

        return self


    def construct_runners(
        self, 
        config_file=None, 
        fastq_dir=None,
        output_directory=None,
        convert_to_realpaths=True,
        submit=False,
    ):

        runners = self.grouped_files.copy()

        for tup, files in self.grouped_files.items():
            tpt, rep = tup 
            full_job = f'{self.job}_{self.timepoint_prefix}{tpt}_{rep}'

            newdir = os.path.join(self.working_directory, full_job) 
            os.mkdir(newdir)

            c = CtgRunner(
                name=full_job,
                config_file=config_file,
                fastq_dir=fastq_dir, 
                fastq1=files[0],
                fastq2=files[1],
                output_directory=newdir,
                convert_to_realpaths=convert_to_realpaths,
            )

            c.create_sge_scripts(
                script_out_path=os.path.join(newdir, 'job.sh'),
                job_name=self.job,
                working_dir_path=newdir, 
                memory=2,
                ncpus=16, 
                commands=[
                    'conda activate ctg-dev', 
                ] 
            )

            if submit: 
                c.sge.submit_script()
                print("submitted!")

            runners[tpt][rep] = c

        self.runners = runners

        return self

    def get_jobids(self): 
        return [r.sge.jobid for r in self.runners]

    # def aggregate_counts(self, wait_for_jobs=False, output_directory=None): 
    #     def get_counts_path(Runner): 
    #         return os.path.join(Runner.output_directory, f"{Runner.name}_counts.txt")

    #     header = None
    #     names = []
    #     dfs = []
    #     for tpt, d1 in self.runners.items(): 
    #         for rep, c in d1.items(): 
    #             counts_file = get_counts_path(c)
    #             df = pd.read_csv(counts_file, sep='\t', comment='#')

    #             if header is None: 
    #                 header = df.iloc[:, :5].set_index(df.columns[0])

    #             df = df.iloc[:, [0,-1]].set_index(df.columns[0])
    #             df.columns = [f'{self.job}_{self.timepoint_prefix}{tpt}_{rep}'] 

    #             dfs.append(df)

    #     combined = pd.concat([header] + dfs, axis=1)

    #     if output_directory is None: 
    #         output_directory = self.working_directory
        
    #     combined.to_csv(
    #         os.path.join(output_directory, f'{self.job}_aggregated_counts.txt'), 
    #         sep='\t',
    #         index_col=None
    #     )

    def aggregate_counts(self): 
        pass

    def to_json(self): 
        """Writes current state to json file"""

        state  = {
            'job': self.__dict__.get('job', ''), 
            'timepoint_prefix': self.__dict__.get('timepoint_prefix', ''), 
            'timepoints': self.__dict__.get('timepoints', []), 
            'replicates': self.__dict__.get('replicates', []), 
            'files': self.__dict__.get('files', ''), 
            #'grouped_files': self.__dict__.get('grouped_files', {}), 
            #'runners': {
            #    k1: {
            #        v2.to_json() for k2,v2 in v1.items()
            #    } for k1,v1 in self.runners.items()
            #}
        }

        return json.dumps(state)

    def create_test_set(self, test_dir = None, test_size=10000): 
        if test_dir is None: 
            test_dir = os.getcwd()

        for f in self.files:
            file_name = os.path.basename(f).split('.')[0]
            with gzip.open(f) as r:
                test_lines = [next(r) for _ in range(test_size)]
            
            with gzip.open(os.path.join(test_dir, f'{file_name}_test.fast.gz'), 'wb') as w: 
                w.write('\n'.join(test_lines))
            

class CtgRunner(object): 
    """Object to create the ctg runner script
    
    Arguments
    ---------
    convert_to_realpaths : bool 
        If True, all paths and directories are checked if they exist
        and will be converted to realpaths
    """
    
    TAB = ' '*4
    
    def __init__(
        self,
        name='ctg', 
        config_file = None, 
        fastq_dir = None, 
        fastq1 = None, 
        fastq2 = None, 
        output_directory = None, 
        convert_to_realpaths = False, 
    ): 
        
        if fastq_dir is None: 
            prefix = ''
        
        else: 
            prefix = fastq_dir

        if fastq1 is None: 
            fastq1 = "None"
        
        if fastq2 is None: 
            fastq2 = "None"

        if not isinstance(fastq1, list): 
            fastq1 = [fastq1]
        
        if not isinstance(fastq2, list): 
            fastq2 = [fastq2]

        if output_directory is None: 
            output_directory = os.getcwd()

        self.name = name
        self.config_file = config_file 
        self.fastq_dir = fastq_dir 
        self.fastq1 = fastq1
        self.fastq2 = fastq2
        self.output_directory = output_directory

        if convert_to_realpaths: 
            self._convert_to_realpaths()

        # Joining multiple fastq files by commas
        self.fastq1 = ','.join([os.path.join(prefix, i) for i in self.fastq1])
        self.fastq2 = ','.join([os.path.join(prefix, i) for i in self.fastq2])


    def _convert_to_realpaths(self):
        """Checks if the path is valid and convert to realpath""" 
        rp = os.path.realpath 

        new_names = []
        for name in [self.config_file, self.output_directory]:
            if os.path.exists(name): 
                new_names.append(rp(name))

        self.config_file, self.output_directory = new_names

        new_names = []
        for name in [self.fastq1, self.fastq2]: 
            new_names.append(
                [rp(i) for i in name if os.path.exists(i)]
            )

        self.fastq1, self.fastq2 = new_names

        return self


    def __repr__(self):
        """Pretty print the command"""

        self.generate_command()

        out_str = self.command[0] + f' \\\n{self.TAB}' + self.command[1] + ' \\\n'
        
        for i,j in pairwise(self.command[2:]):
            out_str += f'{self.TAB*2}{i} {j} \\\n'

        out_str = out_str[:-2] # remove the last newline and "\"

        return out_str


    def generate_command(self):
        self.command = [
            'ctg', 
                'count', 
                    '--config', self.config_file, 
                    '--fastq1', self.fastq1,
                    '--fastq2', self.fastq2,
                    '--output_counts', os.path.join(self.output_directory, f"{self.name}_counts.txt"),
                    '--output_bam', os.path.join(self.output_directory, f"{self.name}_alignments.bam"),  
        ]

        return self


    def create_sge_scripts(
        self, 
        script_out_path='job.sge',
        **kwargs
    ):

        """Create SGE script"""

        if 'commands' in kwargs: 
            commands = kwargs.pop('commands')
            if isinstance(commands, list): 
                commands = '\n'.join(commands) + '\n'
                    
            else: 
                commands += '\n'
            
        else: 
            commands = ''

        commands += self.__repr__()

        self.sge = SgeWriter(commands=commands, **kwargs)
        self.sge.write_script(script_out_path)

        return self


def pairwise(iterable):
    "s -> (s0, s1), (s2, s3), (s4, s5), ..."
    a = iter(iterable)
    
    return zip(a, a)


def edit_distance(str1, str2):
    return sum([1 for i,j in zip_longest(str1, str2) if i != j])

if __name__ == "__main__": 
    c = CtgRunner(
        config_file='configs/config.txt', 
        fastq_dir="/cellar/users/samsonfong/Ongoing/cdk_screens/data/ngs_data/190312_D00611_0677_AHWMNWBCX2_PE75_Mali/Data/Fastq", 
        fastq1 = "MDA_D12_R1_S41_L002_R1_001.fastq.gz", 
        fastq2 = "MDA_D12_R1_S41_L001_R2_001.fastq.gz", 
    )

    #print(c)

    pipeline = CtgPipeline(
        fastq_directory="../data/testset",
        working_directory='test' 
    )

    pipeline.parse_files(
        "MDA_D*",
        timepoint_prefix='D', 
        read_index=5
    )

    pipeline.construct_runners(
        config_file='configs/config.txt', 
        submit=False,
    )

    # pipeline.aggregate_counts()
    
    print(pipeline.to_json())
