from setuptools import setup


def readme():
    with open('README.md') as readme_file:
        return readme_file.read()

setup(
    name='webscrapetools',
    version='0.4.1',
    description='A basic but fast, persistent and threadsafe caching system',
    long_description=readme(),
    url='https://github.com/chris-ch/webscrapetools',
    author='Christophe',
    author_email='chris.perso@gmail.com',
    packages=['webscrapetools'],
    package_dir={'webscrapetools': 'src/webscrapetools'},
    license='Apache',
    download_url='https://github.com/chris-ch/webscrapetools/webscrapetools/archive/0.4.1.tar.gz',
    install_requires=[
        'requests',
    ],
    zip_safe=True
)
