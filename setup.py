from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="uk-police-api-extractor",
    version="0.1.0",
    author="UK Police API Extractor Team",
    author_email="example@example.com",
    description="A Python library for extracting data from the UK Police Data API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/username/uk-police-api-extractor",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.6",
    install_requires=[
        "requests>=2.25.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "pytest-cov>=2.10.0",
            "black>=20.8b1",
        ],
    },
)