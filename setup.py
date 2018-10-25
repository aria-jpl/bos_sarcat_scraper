from setuptools import setup
setup(
    name = 'bos_sarcat_scraper',
    version = '0.0.1',
    packages = ['bos_sarcat_scraper'],
    entry_points = {
        'console_scripts': [
            'bos_sarcat_scraper = bos_sarcat_scraper.__main__:main'
        ]
    },
    install_requires= [ 'shapely', 'geojson']
)
