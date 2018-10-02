try:
    # Python3
    from urllib.parse import urlencode
except ImportError:
    # Python2
    from urllib.parse import urlencode


def build_url(path, *params, **kwparams):
    final_params = []
    if params:
        final_params.append('&'.join(params))
    if kwparams:
        final_params.append(urlencode(kwparams))

    if not final_params:
        return path

    return path + '?' + '&'.join(final_params)
