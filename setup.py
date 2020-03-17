from setuptools import setup, find_packages


# Dependencies for using the library
install_requires = [
    'pathlib>=1.0.1',
    'pandas>=0.25.1',
    'streamlit>=0.56.0',
    'tcpdump_processing @ git+https://github.com/mbakholdina/lib-tcpdump-processing.git@master#egg=tcpdump_processing',
]

setup(
    name='lib-srt-stats-analysis',
    version='0.1',
    author='Maria Sharabayko',
    author_email='maria.bakholdina@gmail.com',
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'join-stats = srt_stats_analysis.join_stats:main'
        ],
    },
)