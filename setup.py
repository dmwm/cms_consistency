import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname), "r").read()

def get_version():
    g = {}
    exec(open("version.py", "r").read(), g)
    return g["Version"]

setup(
    name = "cms_consistency",
    version = get_version(),
    author = "Igor Mandrichenko",
    author_email = "ivm@fnal.gov",
    description = ("Rucio consistency enforcement for CMS",),
    license = "BSD 3-clause",
    url = "https://github.com/rucio/cms_enforcement",
    packages=["cms_consistency", "cms_consistency.site_ctl", 'actions', "monitor", "site_ce", "wm"],
    long_description="Rucio consistency enforcement for CMS", #read('README'),
    zip_safe = False,
    entry_points = {
        "console_scripts": [
            "site_ctl = cms_consistency.site_ctl:main"
        ]
    }
)
