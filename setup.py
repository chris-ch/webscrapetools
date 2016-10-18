from setuptools import setup


def readme():
    with open('README.txt') as readme_file:
        return readme_file.read()

setup(
    name='webscrapetools',
    version='0.3',
    description='A basic but threadsafe caching system',
    long_description=readme(),
    url='https://github.com/chris-ch/webscrapetools',
    author='Christophe',
    author_email='chris.perso@gmail.com',
    packages=['webscrapetools'],
    package_dir={'webscrapetools': 'src/webscrapetools'},
    license='Apache',
    install_requires=[
        'requests',
    ],
    zip_safe=True
)
