import os
import re
from multiprocessing import cpu_count

###############################################################################
# Default settings for gdc-client
###############################################################################

USER_DEFAULT_CONFIG_LOCATION = os.path.expanduser(os.path.join("~", ".config.dtt"))

####################
# API defaults
####################

# The base url for the tcp (http) client
tcp_url = "https://api.gdc.cancer.gov/"

####################
# Multiprocessing
####################

# The number of processes used to download data files
processes = min(cpu_count(), 8)

HTTP_CHUNK_SIZE = 1024 * 1024  # 1 MB
SAVE_INTERVAL = 1024 * 1024 * 1024  # 1 GiB
# Part size for multipart uploads
UPLOAD_PART_SIZE = 1024 * 1024 * 1024  # 1 GiB

# The following file will contain superseded files information
SUPERSEDED_INFO_FILENAME_TEMPLATE = re.compile(r"superseded_files[\d._]+.txt")
