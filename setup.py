from setuptools import setup, find_packages

setup(
    name="dpn_pipe_line_sdk",
    version="0.1",
    description="Bundle for utils and dpn_observability_sdk",
    author="Hariharan MS",
    packages=[
        "utils",
        "dpn_observability_sdk",
    ],
    include_package_data=True,
    install_requires=[
        # add dependencies here if needed
    ],
)