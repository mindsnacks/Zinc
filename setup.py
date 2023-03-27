from setuptools import setup, find_packages
import os

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
NEWS = open(os.path.join(here, 'NEWS.txt')).read()

version = '0.2.0'

install_requires = [
    # List your project dependencies here.
    # For more details, see:
    # http://packages.python.org/distribute/setuptools.html#declaring-dependencies
    "toml==0.8.1",
    "lockfile==0.9.1",
    "boto==2.8.0",
    "atomicfile==0.1",
    "redis==4.5.3",
    "jsonschema==1.3.0",
    "typecheck==0.3.5",
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
      author_email='andy@mrox.net',
      url='',
      license='MIT License',
      packages=find_packages('src'),
      package_dir = {'': 'src'}, include_package_data = True,
      package_data = {'': ['*.json']},
      zip_safe=False,
      install_requires=install_requires,
      entry_points={
          'console_scripts':
          ['zinc=zinc.cli:main']
      }
      )
