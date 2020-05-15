try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements

from setuptools import (
    find_packages,
    setup,
)


setup(
    name='rqalpha_mod_kxg',     #mod名
    version="0.1.0",
    description='RQAlpha Mod Use DB datasource',
    packages=find_packages(exclude=[]),
    author='your name',
    author_email='your email address',
    license='Apache License v2',
    package_data={'': ['*.*']},
    url='https://github.com/chenjiajia/rqalpha_mod_kxg',
    install_requires=[str(ir.requirement) for ir in parse_requirements("requirements.txt", session=False)],
    zip_safe=False,
    classifiers=[
        'Programming Language :: Python',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: Unix',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)