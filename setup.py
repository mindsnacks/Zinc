from setuptools import setup, find_packages
import os

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
NEWS = open(os.path.join(here, 'NEWS.txt')).read()

version = '0.2.1'

install_requires = [
    # List your project dependencies here.
    # For more details, see:
    # http://packages.python.org/distribute/setuptools.html#declaring-dependencies
    "toml==0.10.0",
    "lockfile==0.9.1",
    "boto3==1.26.160",
    "atomicwrites==1.3.0",
    "redis==4.4.4",
    "jsonschema==1.3.0",
]


setup(name='zinc',
      version=version,
      description="Keep your files shiny and healthy.",
      long_description=README + '\n\n' + NEWS,
      classifiers=[
          # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      ],
      keywords='',
      author='Andy Mroczkowski',
      author_email='a@mrox.co',
      url='',
      license='MIT License',
      packages=find_packages('src'),
      package_dir={'': 'src'}, include_package_data=True,
      package_data={'': ['*.json']},
      zip_safe=False,
      install_requires=install_requires,
      entry_points={
          'console_scripts':
          ['zinc=zinc.cli:main']
      }
      )
