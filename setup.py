from distutils.core import setup

requirements = [
    "natsort",
    "pandas",
    # "matplotlib",
    # "statmodels",
    "tqdm",
    # "sqlalchemy"
]

setup(
    name='dsa',
    version='1.0',
    description='Digital School Analysis',
    author='Vitaly Romanov',
    author_email='mortiv16@gmail.com',
    # url='https://www.python.org/sigs/distutils-sig/',
    packages=['dsa'],
    package_dir = {'': 'src/python'},
    install_requires=requirements
)
