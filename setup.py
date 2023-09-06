from setuptools import setup

setup(
    name='ncdu-bos',
    version='0.0.1',
    py_modules=['ncdu-bos'],
    install_requires=[
        'click==8.1.7',
        'bce-python-sdk==0.8.90'
    ],
    entry_points='''
        [console_scripts]
        ncdu-bos=ncdu_bos:main
    ''',
)
