from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    install_requires=[
        "certifi==2020.6.20",
        "chardet==3.0.4",
        "idna==2.10",
        "requests==2.24.0",
        "tqdm==4.48.2",
        "urllib3==1.25.10",
    ],
    name="pbiapi",  # Replace with your own username
    version="0.1.0",
    author="Chris Kucharczyk",
    author_email="ckucharczyk@ideo.com",
    description="PowerBI API wrapper",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ideo/ideo-pbiapi",
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
)
