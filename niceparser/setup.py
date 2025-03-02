from setuptools import setup, find_packages

setup(
    name="balancestore",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "judy>=0.1.6",
        "apsw>=3.39.0",
        "lz4>=4.3.2",
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="High-performance UTXO balance management system",
    keywords="blockchain, utxo, balance, performance",
    python_requires=">=3.8",
)
