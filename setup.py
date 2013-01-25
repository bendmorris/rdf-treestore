from setuptools import setup
from __init__ import __version__


setup(name='treestore',
      version=__version__,
      description='store phylogenetic trees in Redland triple stores',
      author='Ben Morris',
      author_email='ben@bendmorris.com',
      url='https://github.com/bendmorris/treestore',
      packages=['treestore'],
      package_dir={
                'treestore':''
                },
      entry_points={
        'console_scripts': [
            'treestore = treestore.treestore:main',
        ],
      },
      )
