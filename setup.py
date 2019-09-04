from setuptools import setup
from os import path

__version = '0.4.4'

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='webscrapetools',
    version=__version,
    description='A basic but fast, persistent and threadsafe caching system',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/chris-ch/webscrapetools',
    author='Christophe',
    author_email='chris.perso@gmail.com',
    packages=['webscrapetools'],
    package_dir={'webscrapetools': 'src/webscrapetools'},
    license='Apache',
    download_url='https://github.com/chris-ch/webscrapetools/webscrapetools/archive/{0}.tar.gz'.format(__version),
    install_requires=[
        'requests',
    ],
    zip_safe=True
)
