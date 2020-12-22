from setuptools import find_packages
from setuptools import setup

setup(
    name="reddit_experiments",
    description="reddit's python experiments framework",
    author="reddit",
    license="BSD",
    use_scm_version=True,
    packages=find_packages(),
    python_requires=">=3.6.2",
    setup_requires=["setuptools_scm"],
    install_requires=["baseplate>=1.5"],
    package_data={"baseplate": ["py.typed"]},
    zip_safe=True,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries",
    ],
)
