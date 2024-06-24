# NolanM Quantitative Trading Environment Package Set Up

## Overview

NolanMQuantTradingEnvSetUp is a Python package designed to set up a local virtual environment with all necessary dependencies specified in the `requirements.txt` file. This package is ideal for quickly setting up your development environment for Quantitative Trading Projects.

---

## Installation

To install this package, simply use pip:

```python
    pip install NolanMQuantTradingEnvSetUp
```

---

## Update Package
### A. Prerequisite
1. Install rustup for updating package only
```link
https://rustup.rs/
```

2. Install neccessary package for updating package only

```sh
    pip install --upgrade setuptools pip
    pip install wheel twine
```

### B. Steps
#### 1. Update requirements.txt
Note: Ensure that all required dependencies are listed in requirements.txt. The file should be UTF-8 encoded.

<b>requirement.txt</b>

```plaintext
    plotly
    python-dotenv
    streamlit
    pandas
    pyspark
    yfinance
    psycopg2
    kafka-python
    alpha-vantage
```

#### 2. Update the Version of package release in setup.py (Not in NolanMQuantTradingEnvSetUp directory)
Open directory in command line with admintrator permission type

```python
    setup(
    name='NolanMQuantTradingEnvSetUp',
    version='2.0',       <---------- Change the Version Here
    packages=find_packages(),
    include_package_data=True,
    install_requires=read_requirements(),
    entry_points={
        'console_scripts': [
            'setup-env=NolanMQuantTradingEnvSetUp.setup:main',
        ],
    },
)
```


#### 3. Build the Distribution
Open directory in command line with admintrator permission type

```sh
    python setup.py sdist bdist_wheel
```

#### 4. Upload to PyPI
Open directory in command line with admintrator permission type

```sh
    twine upload dist/*
```

Input the API keys and Enter