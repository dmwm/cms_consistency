from .version import Version

__version__ = Version
version_info = (tuple(int(x) for x in Version.split(".")) + (0,0,0))[:3]            # pad with 0s
