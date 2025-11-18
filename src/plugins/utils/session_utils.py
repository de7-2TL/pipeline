from curl_cffi import requests, Session


def get_session() -> Session:
    """
    Get a requests session that impersonates a Chrome browser.
    :return: Session
    :rtype:
    """

    return requests.Session(impersonate="chrome")
