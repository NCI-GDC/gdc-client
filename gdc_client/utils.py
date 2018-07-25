from urllib import urlencode


def url_with_params(path, *params, **kwparams):
    final_params = []
    if params:
        final_params.append('&'.join(params))
    if kwparams:
        final_params.append(urlencode(kwparams))

    if not final_params:
        return path

    return path + '?' + '&'.join(final_params)
