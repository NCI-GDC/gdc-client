# ***************************************************************************************
# Title: LabAdvComp/parcel
# Author: Joshua S. Miller
# Date: May 26, 2016
# Code version: 0.1.13
# Availability: https://github.com/LabAdvComp/parcel
# ***************************************************************************************

import platform
from termcolor import colored as _colored

OS_WINDOWS = False
OS_LINUX = False
OS_OSX = False

# Are we running on windows?
if platform.system() == "Windows":
    OS_WINDOWS = True
elif platform.system() == "Darwin":
    OS_OSX = True
elif platform.system() == "Linux":
    OS_LINUX = True

# Are we running on windows?
if OS_WINDOWS or OS_OSX:
    from threading import Thread as Process
else:
    # Assume a posix system
    from multiprocessing import Process


def colored(text, color):
    if OS_WINDOWS:
        return text
    else:
        return _colored(text, color)
