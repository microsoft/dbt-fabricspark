import os
import sys
import site
from setuptools import setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" ' "and try again")
    sys.exit(1)

base_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
print(base_dir)
sys.path.append(base_dir)
os.chdir(base_dir)

from dbt.adapters.fabricspark.__version__ import version

with open(os.path.join(base_dir, "CHANGELOG.md"), "r", encoding="utf8") as f:
    long_description = f.read()

setup(
    name='dbt-fabricspark-custom',
    version=version,
    description="The Apache Spark adapter plugin for dbt",
    long_description=long_description,
    packages=find_namespace_packages(include=["dbt", "dbt.*","dbt_common.*"]),
    package_data={
        'dbt.include.fabricspark': ["macros/**/*",'dbt_project.yml','profile_template.yml'],
    },
    include_package_data=True,
    install_requires=["dbt-core==1.9.1", "dbt-fabric==1.9.0", "dbt-spark==1.9.0"],
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8",
)
