from setuptools import find_packages, setup

setup(
    name="dagster_alphasrc",
    packages=find_packages(exclude=["dagster_alphasrc_tests"]),
    install_requires=[
        "dagster",
        "dagster-webserver",
        "pandas",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
