from setuptools import find_packages
from setuptools import setup

setup(
    name="reddit_experiments",
    description="reddit's python experiments framework",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/reddit/experiments.py",
    project_urls={
        "Documentation": "https://reddit-experiments.readthedocs.io/",
    },
    author="reddit",
    license="BSD",
    use_scm_version=True,
    packages=find_packages(),
    python_requires=">=3.7",
    setup_requires=["setuptools_scm"],
    install_requires=[
        "baseplate>=2.0.0a1,<3.0",
        "reddit-edgecontext>=1.0.0a3,<2.0",
        "reddit-decider>=1.2.2",
    ],
    package_data={"reddit_experiments": ["py.typed"]},
    zip_safe=True,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Libraries",
    ],
)
