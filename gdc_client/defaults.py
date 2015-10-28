###############################################################################
# Default settings for gdc-client
###############################################################################

####################
# API defaults
####################

# The base url for the tcp (http) client
tcp_url = 'https://gdc-api.nci.nih.gov/'

# The base url for the tcp (http) client
udt_url = 'https://gdc-parcel.nci.nih.gov/'

####################
# Multiprocessing
####################

# The number of processes used to download data files
processes = 8

####################
# UDT Proxy settings
####################

# This is the host where the UDT proxy (parcel) will attempt to bind a
# proxy to traffic to the GDC api
proxy_host = 'localhost'

# This is the port where the UDT proxy (parcel) will attempt to bind a
# proxy (on `proxy_host`) to traffic on the GDC api host
proxy_port = 9000

part_size = 1073741824 # 1GiB