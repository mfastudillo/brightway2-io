import hashlib
import json
import os
import pprint


def activity_hash(data):
    """Hash an activity dataset.

    Used to import data formats like ecospold 1 (ecoinvent v1-2) and SimaPro, where no unique attributes for datasets are given. This is clearly an imperfect and brittle solution, but there is no other obvious approach at this time.

    Uses the following, in order:
        * *name* Lower case, defult is ``""`` (empty string).
        * *categories* In string form, joined together with ``""`` (empty string), default is ``[]``.
        * *unit* Lower case, default is ``""`` (empty string).
        * *location* Lower case, default is ``""`` (empty string).

    Args:
        * *data* (dict): The :ref:`activity dataset data <database-documents>`.

    Returns:
        A MD5 hash string, hex-encoded.

    """
    string = ((data.get(u"name") or u"").lower() +
              u"".join(data.get(u"categories") or []) +
              (data.get(u"unit") or u"").lower() +
              (data.get(u"reference product") or u"").lower() +
              (data.get(u"location") or u"").lower())
    return unicode(hashlib.md5(string.encode('utf-8')).hexdigest())


def es2_activity_hash(activity, flow):
    """Generate unique ID for ecoinvent3 dataset.

    Despite using a million UUIDs, there is actually no unique ID in an ecospold2 dataset. Datasets are uniquely identified by the combination of activity and flow UUIDs."""
    return unicode(hashlib.md5((activity + flow).encode('utf-8')).hexdigest())


def load_json_data_file(filename):
    DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
    if filename[-5:] != ".json":
        filename = filename + ".json"
    return json.load(open(os.path.join(DATA_DIR, filename)))


def format_for_logging(obj):
    return pprint.pformat(obj, indent=2)