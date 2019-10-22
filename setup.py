from setuptools import setup

__version = '0.5.4'

INSTALL_REQUIRE = ['requests>=2.20.0']

setup(
    name='webscrapetools',
    version=__version,
    description='A basic but fast, persistent and threadsafe caching system',
    long_description=open('README.rst').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/chris-ch/webscrapetools',
    author='Christophe',
    author_email='chris.perso@gmail.com',
    packages=['webscrapetools'],
    package_dir={'webscrapetools': 'src/webscrapetools'},
    license='Apache',
    download_url='https://github.com/chris-ch/webscrapetools/webscrapetools/archive/{0}.tar.gz'.format(__version),
    install_requires=INSTALL_REQUIRE,
    zip_safe=True
)
