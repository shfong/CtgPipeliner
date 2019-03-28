from CtgPipeliner import CtgPipeline

import os
import pytest


def test_parse_files(tmpdir): 
    # Create temporary files

    timepoints = [(3,3), (6,)]

    file_names = []
    for i in timepoints:
        for k, j in enumerate(i): 
            for l in ['R1', 'R2']: 
                file_names.append(os.path.join(tmpdir, f'test_T{j}_{k+1}_{l}.fastq.gz'))

    for f in file_names: 
        with open(os.path.join(tmpdir, f), 'w') as f: 
            f.write('') 

    pipeline = CtgPipeline(
        fastq_directory=tmpdir,
    ).parse_files(
        os.path.join(tmpdir, 'test_*'),
        timepoint_prefix='T',
    )   

    assert pipeline.job == 'test'
    assert pipeline.files == set(file_names)
    
    expected_dict = {
        ('T3', '1'): [[file_names[0]], [file_names[1]]], 
        ('T3', '2'): [[file_names[2]], [file_names[3]]], 
        ('T6', '1'): [[file_names[4]], [file_names[5]]],
    }

    assert pipeline.grouped_files == expected_dict