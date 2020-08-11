from setuptools import setup

setup(name='mongorm',
      version='0.1',
      description='Object oriented ORM for PyMongo and MongoDB',
      url='',
      author='Ismael Carreno Mercado',
      author_email='ismacm@outlook.com',
      license='MIT',
      packages=['orm'],
      python_requires='>3.6.0',
      install_requires=[
          'pymongo',
          'pymodm',
          'mongomock'
      ],
      zip_safe=False)