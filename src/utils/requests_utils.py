import requests
import logging


def try_get_request(url, timeout):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.exceptions.Timeout:
        logging.error("Request timed out")
        raise
    except requests.exceptions.RequestException as e:
        logging.error(f"Error occurred {e}")
        raise
