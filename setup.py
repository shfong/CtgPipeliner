from setuptools import setup, find_packages

import re

# Get version number 
VERSIONFILE="nbgwas/version.py"
verstrline = open(VERSIONFILE, "rt").read()
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VSRE, verstrline, re.M)
if mo:
    verstr = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))


with open('README.rst') as readme_file:
    readme = readme_file.read()


setup(
    name='ctg-pipeliner',
    version=verstr,
    description='Automatic CTG Pipeline',
    url='https://github.com/shfong/CtgPipeliner',
    author='Samson Fong',
    author_email='shfong@ucsd.edu',
    license='MIT',
    long_description=readme + '\n\n' + history,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Science/Research',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    packages=find_packages(exclude=['os', 're', 'time']),
    install_requires=[
        'numpy', 
        'pandas',
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest']
)
