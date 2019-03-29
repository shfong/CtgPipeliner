import os
import subprocess
import json

class SgeWriter(object): 
    """Object to create sge scripts"""

    def __init__(
        self, 
        job_name='job',
        stdout_path=None, 
        stderr_path=None, 
        working_dir_path=None, 
        memory=2, # Expect this to be an int (GB of memory for h_vmem) 
        ncpus=1,
        n_array_jobs = None, 
        commands = [], 
        verbose=True,
        validate_paths=True,
        convert_to_realpaths=True,
    ):
        
        if working_dir_path is None: 
            working_dir_path = os.getcwd()

        if stdout_path is None:
            stdout_path = os.path.join(working_dir_path, job_name + '.e')
        elif os.path.isdir(stdout_path): 
            stdout_path = os.path.join(stdout_path, job_name + '.e')

        if stderr_path is None: 
            stderr_path = os.path.join(working_dir_path, job_name + '.o')
        elif os.path.isdir(stderr_path): 
            stderr_path = os.path.join(stderr_path, job_name + '.o')
        

        if validate_paths: 
            for f in [stdout_path, stderr_path, working_dir_path]: 
                self._file_exist(f, dirname=True)

        if convert_to_realpaths:
            stdout_path = os.path.realpath(stdout_path)
            stderr_path = os.path.realpath(stderr_path)
            working_dir_path = os.path.realpath(working_dir_path)


        if not isinstance(commands, list): 
            commands = [commands]

        self.job_name = job_name
        self.stdout_path = stdout_path 
        self.stderr_path = stderr_path
        self.working_dir_path = working_dir_path
        self.memory = memory 
        self.ncpus = ncpus
        self.n_array_jobs = n_array_jobs
        self.commands = commands 
        self.verbose = verbose

        self._status = None

    def __repr__(self): 
        self.generate_script()

        return '\n'.join(self.script)


    def _file_exist(self, filepath, dirname=False):
        if dirname: 
            filepath = os.path.dirname(filepath)

        if os.path.exists(filepath): 
            return True 

        else: 
            raise FileNotFoundError(f"{filepath} not found!")


    def generate_script(self):
        self.script = [
            '#! /bin/bash',
            '',
            '#$ -S /bin/bash',
            '#$ -V', # Transfer all environment variables to job script
            f'#$ -N {self.job_name}',
            f'#$ -o "{self.stdout_path}"',
            f'#$ -e "{self.stderr_path}"',
            f'#$ -wd "{self.working_dir_path}"',
            f'#$ -pe smp {self.ncpus}', 
            f'#$ -l h_vmem={self.memory}G',
            ''
        ]

        if self.n_array_jobs is not None: 
            self.script.extend(
                [f'#$ -t 1-{self.n_array_jobs}', '']
            )

        header, tail  = [], []
        if self.verbose: 
            header = [
                'echo "***Running on host: $HOSTNAME"', 
                'echo "***Job ID: $JOB_ID"',
                'echo "***Job started on: $(date)"', 
            ]

            tail = [
                'echo "***Job ended on: $(date)"',
                r'echo -e "***Job information:\n$(qstat -j $JOB_ID)"'
                '' 
            ]

        self.script.extend(header + self.commands + tail)

        return self


    def read_commands(self, filepath, overwrite=False):
        with open(filepath) as f: 
            new_commands = f.readlines()

        if overwrite: 
            self.commands = new_commands 

        else: 
            self.commands.extend(new_commands)


    def write_script(self, outpath): 
        with open(outpath, 'w') as f: 
            f.write(self.__repr__())

        self.sge_script_path = outpath 

    
    def submit_script(self): 
        if not hasattr(self, "sge_script_path"):
            raise ValueError("No sge script found!") 

        
        self.CompletedProcess = subprocess.run(
            ["qsub", self.sge_script_path], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
        )

        if self.CompletedProcess.stderr: 
            raise RuntimeError("Submission error encountered! %s" % self.CompletedProcess.stderr)

        self.jobid = int(self.CompletedProcess.stdout.split()[2])

        self._status = 'submitted'

    @property
    def status(self):
        if self._status in [None, 'killed']: 
            return self._status 

        qstat = subprocess.Popen(["qstat"], stdout=subprocess.PIPE)
        output = subprocess.run(
            ['grep', str(self.jobid)], 
            stdin=qstat.stdout,
            stdout=subprocess.PIPE, 
            check=False,
        ).stdout

        out = output.split()
        if out: 
            self._status = out[4].decode('ascii')

        else: 
            self._status = 'finished'

        return self._status

    def kill_job(self): 
        if not hasattr(self, "jobid"): 
            raise ValueError("Job has not been submitted yet!")

        subprocess.run([
            'qdel', str(self.jobid)
        ])

        self._status = 'killed'

    def to_json(self, outpath=None): 
        state = {
            'job_name': self.__dict__.get('job_name', ''),
            'stdout_path': self.__dict__.get('stdout_path', '') ,
            'stderr_path': self.__dict__.get('stderr_path', ''), 
            'working_dir_path': self.__dict__.get('working_dir_path', ''),
            'memory': self.__dict__.get('memory', None),
            'ncpus': self.__dict__.get('ncpus', None), 
            'n_array_jobs': self.__dict__.get('n_array_jobs', None),
            'commands': self.__dict__.get('commands', []),
            'verbose': self.__dict__.get('verbose', None), 
            'status': self.status,  
        }

        if outpath is not None: 
            with open(outpath, 'w') as f: 
                json.dump(state, f, indent=4, separators=(',', ':'))

        else: 
            return json.dumps(state, indent=4, separators=(',', ':'))

    @classmethod
    def from_json(cls, json_path=None, json_str=None): 
        if sum([1 for i in  [json_path, json_str] if i is None]) != 1: 
            raise ValueError("Can only take either json_path or json_str")

        if json_path is not None: 
            with open(json_path) as f: 
                state = json.load(f)

        if json_str is not None: 
            state = json.loads(json_str)
        
        obj = cls()
        obj.__dict__.update(state)

        return obj

if __name__ == "__main__": 
    sge = SgeWriter(
        job_name='MDA',
        stderr_path='test', 
        stdout_path='test', 
        validate_paths=False, 
    )

    sge.commands = [
        'sleep 300'
    ]
    print(sge)
    sge.write_script('test_out.sh')

    #sge.submit_script()

    sge.to_json(outpath='test.sh') 
