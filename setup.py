import setuptools

setuptools.setup(
    name="stream-processor",
    version="0.0.1",
    description="Library for stream processing",
    url="https://github.com/shuttl-tech/shuttl_workflows",
    author="Shuttl",
    author_email="rameez.shuhaib@shuttl.com",
    license="MIT",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    extras_require={
        "test": ["pytest", "pytest-runner", "pytest-cov", "pytest-pep8", "hypothesis"]
    },
)
