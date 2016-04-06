import argparse
import urlparse

import requests

# Because this is such beautiful...
from bs4 import BeautifulSoup as bs


try:
    import http.client as http_client
except ImportError:
    # Python 2
    import httplib as http_client
http_client.HTTPConnection.debuglevel = 1

def token(args):
    """ Generate and / or retrieve a GDC auth token.
    """
    session = requests.Session()
    session.keep_alive = False

    # GET GDC session and find ourselves an eRA session.
    res = session.head('https://gdc-portal.nci.nih.gov/auth/',
        allow_redirects=True,
    )
    res.raise_for_status()

    # Extract the eRA session information from query string args...
    url = urlparse.urlparse(res.url)
    qs = urlparse.parse_qs(url[4])

    # TODO parse these from form hidden inputs
    data = {
        'SMLOCALE': 'US-EN',
        'SMENC': 'ISO-8859-1',
        'smquerydata': '',
        'smagentname': '-SM-itrusteauth.nih.gov',
        'postpreservationdata': '',
        'target': qs['TARGET'],
        'minloa': 'NIHIssuedLOA4',
    }

    # Inject eRA credentials.
    data['USER'] = args.username
    data['PASSWORD'] = args.password

    # Validate against eRA w/ the above...
    res = session.post('https://itrusteauth.nih.gov/siteminderagent/forms/login.fcc',
        data=data,
    )
    res.raise_for_status()

    # Forward the SAML response to the GDC API for validation...
    soup = bs(res.text, 'html.parser')
    saml = soup.input['value']

    data = {
        'SAMLResponse': saml,
    }

    res = session.post('https://gdc-portal.nci.nih.gov/auth/Shibboleth.sso/SAML2/POST',
        data=data,
    )
    res.raise_for_status()

    # And finally, GET the GDC auth token from API.
    res = session.get('https://gdc-portal.nci.nih.gov/auth/token')
    res.raise_for_status()

    # TA-DAAA! You've got a GDC auth token via command line...
    print(res.text)

    # ...I'm going to sleep now...

def config(parser):
    """ Configure a parser for token generation.
    """
    parser.set_defaults(func=token)

    parser.add_argument('-u', '--username',
        required=True,
        help='eRA username',
    )

    # FIXME TODO UT FACIAM REMOVE THIS BEFORE USE
    # ===========================================
    # we don't want plain-text passwords being stored in logs
    # consider using a password file or interactive prompt
    parser.add_argument('-p', '--password',
        required=True,
        help='eRA password',
    )
