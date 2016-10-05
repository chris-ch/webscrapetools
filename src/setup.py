from setuptools import setup

setup(
    name='webscrapetools',
    version='0.1',
    description='A basic but threadsafe caching system',
    url='https://github.com/chris-ch/webscrapetools',
    author='Christophe',
    author_email='chris.perso@gmail.com',
    packages=[''],
    package_dir={'': 'src'},
    license='Apache',
    install_requires=[
        'requests',
    ],
    zip_safe=True
)
