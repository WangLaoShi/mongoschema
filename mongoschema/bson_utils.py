"""Helper functions to encode python data types
to bson data types. A lot has been borowed from 
the bson modul in pymongo."""
import datetime
import re

import bson
from bson.objectid import ObjectId
from bson.decimal128 import Decimal128

from .logger import logger


__all__ = ["get_dtype"]


def get_dtype(value):
    """
    """
    # logger.debug("Creating bson mapping for {}, type {}".format(value, type(value)))
    if value == None:
        return "null"
    elif isinstance(value, ObjectId):
        return "ObjectId"
    elif isinstance(value, int) and type(value) != bool:
        return _get_int(value)
    elif isinstance(value, Decimal128):
        return "decimal128"
    mapper= {
            bool: "bool",
            datetime.date: "Date",
            datetime.datetime: "Date",
            bson.regex.Regex: "Regex",
            str: "string",
            float: "float",
            list: "array",
            dict: "object",
            bytes: "binData"
    }
    return mapper[type(value)]

def _get_int(value):
    """Encode python int type to bson int32/64."""
    if -2147483648 <= value <= 2147483647:
        return "int"
    else:
        return "long"

def _get_float(value):
    pass
