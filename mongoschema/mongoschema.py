import csv
from collections import OrderedDict
import json

from pymongo import MongoClient
from texttable import Texttable

from .logger import logger
from .bson_utils import get_dtype


# Export
__all__ = ['SchemaAnalyzer']


class SchemaAnalyzer(object):
    """SchemaAnalyzer is the main class for building 
    a MongoDB schema analysis.

    The API is simple, instantiate a schema object: 
    >>> schema = SchemaAnalyzer()
    >>> schema.info()

    You can export the schema to CSV:

    >>> schema.to_csv()

    The `to_csv()` method is just a wrapper around the 
    csv writer method from the standard library, so you can
    pass in all the arguments you would pass to your csv writer.
    >>> schema.to_csv(delimiter=';')
    >>> schema.save()
    """
    def __init__(self, host="", db="", collection="", query={}, **kwargs):
        """
        TODO: solve working with new query
        The init metod provides some 'private' props, like `_len`, the 
        length of the query results
        """
        if not host:
            self.cursor = MongoClient()
        else:
            self.cursor = MongoClient(host, **kwargs)
        self.db = db
        self.collection = collection
        self.query = query
        self.schema = {}
        self._len = 0

    def analyze(self):
        """
        TODO: easier collection selection
        TODO: make it poosible to analyze the db
        """
        logger.info("Analyzing schema.")
        conn = self.cursor[self.db][self.collection]
        results = list(conn.find(self.query))  # 立即获取所有结果
        
        # Analyze the results
        logger.debug("Analyzing {} in {}".format(self.collection, self.db))
        for result in results:
            self._len += 1
            self._process_object(result)
        # Calculate the percentage for each field
        self._preprocesss_for_reproting()

    """
    New and better method for single objects
     def get_keys(item):
    ...:     for k,v in item.items():
    ...:         if isinstance(v, dict):
    ...:             get_keys(v)
    ...:         elif isinstance(v, list):
    ...:             for e in v:
    ...:                 get_keys(e)
    ...:         else:
    ...:             if not i.get(k):
    ...:                 i[k] = 1
    """

    def _process_object(self, result):
        """
        """
        global curr_object
        curr_object = {}
        self._get_from_object(result)
        
        def get_nested_value(obj, path):
            """获取嵌套路径的值"""
            current = obj
            for key in path.split('.'):
                if isinstance(current, dict):
                    current = current.get(key)
                elif isinstance(current, list):
                    # 如果是数组，收集所有元素的值
                    values = []
                    for item in current:
                        if isinstance(item, dict):
                            values.append(item.get(key))
                    current = values
                else:
                    return None
            return current

        for key in curr_object.keys():
            if self.schema.get(key):
                self.schema[key]["sum"] += 1
                # 获取嵌套路径的值
                values = get_nested_value(result, key)
                if values is not None:
                    if "values" not in self.schema[key]:
                        self.schema[key]["values"] = {}
                    # 处理单个值或数组值
                    if not isinstance(values, list):
                        values = [values]
                    for value in values:
                        if value is not None:
                            value_str = str(value)
                            self.schema[key]["values"][value_str] = self.schema[key]["values"].get(value_str, 0) + 1
                else:
                    self.schema[key]["values"] = {}
            else:
                self.schema[key] = curr_object[key]
                # 初始化值统计
                values = get_nested_value(result, key)
                if values is not None:
                    if not isinstance(values, list):
                        values = [values]
                    self.schema[key]["values"] = {}
                    for value in values:
                        if value is not None:
                            value_str = str(value)
                            self.schema[key]["values"][value_str] = 1
                else:
                    self.schema[key]["values"] = {}

    def _get_from_object(self, result, path=[]):
        """
        The main method that creates the schema
        """
        for key, val in result.items():
            new_path = path + [key]
            if isinstance(val, dict):
                # 处理嵌套字典
                self._get_from_object(val, path=new_path)
            elif isinstance(val, list):
                # 处理嵌套列表
                self._get_from_list(val, path=new_path)
            else:
                # 处理叶子节点
                full_path = '.'.join(new_path)
                if full_path not in curr_object:
                    curr_object[full_path] = {
                        'type': get_dtype(val),
                        'sum': 1,
                        'values': {str(val): 1}
                    }
                else:
                    curr_object[full_path]['sum'] += 1
                    value_str = str(val)
                    if 'values' not in curr_object[full_path]:
                        curr_object[full_path]['values'] = {}
                    curr_object[full_path]['values'][value_str] = curr_object[full_path]['values'].get(value_str, 0) + 1

    def _get_from_list(self, results, path=[]):
        """
        Maps all elements of a list with the method `_get_from_object`.
        """
        for result in results:
            if isinstance(result, dict):
                # 处理列表中的字典
                self._get_from_object(result, path=path)
            elif isinstance(result, list):
                # 处理嵌套列表
                self._get_from_list(result, path=path)
            else:
                # 处理列表中的基本类型值
                full_path = '.'.join(path)
                if full_path not in curr_object:
                    curr_object[full_path] = {
                        'type': get_dtype(result),
                        'sum': 1,
                        'values': {str(result): 1}
                    }
                else:
                    curr_object[full_path]['sum'] += 1
                    value_str = str(result)
                    if 'values' not in curr_object[full_path]:
                        curr_object[full_path]['values'] = {}
                    curr_object[full_path]['values'][value_str] = curr_object[full_path]['values'].get(value_str, 0) + 1

    def _preprocesss_for_reproting(self):
        """Prepares the date for reporting to stdOut or JSON"""
        # Add percentage for field
        for value in self.schema.values():
            percentage = round(value["sum"]*100/self._len, 2)
            value["occurrence"] = str(percentage) + " %"
        # Prepare the data
        # first sorting by occurence
        # than changin occurence to %
        data = sorted(
            self.schema.items(),
            key=lambda x: x[1]["sum"],
            reverse=True
        )
        # Insert into OrderdDict
        # TODO exception for python 3.6 >
        self.schema = OrderedDict()
        for element in data:
            key, value = element
            self.schema[key] = value

    def _get_rows(self):
        """Returns array coresoinding to rows."""
        rows = []
        for key, value in self.schema.items():
            name = key
            type_ = value["type"]
            occurrence = value["occurrence"]
            rows.append([name, type_, occurrence])
        return rows

    def to_json(self):
        """JSON representation of the schema.
        TODO: the first line of to_json an __str__ duplicate code, refactor
        """
        if not self.schema:
            self.analyze()
        return json.dumps(self.schema)

    def to_csv(self, name="report.csv", **kwargs):
        """CSV representation of the schema."""
        if not self.schema:
            self.analyze()
        with open(name, "w") as f:
            csv_writer = csv.writer(f, **kwargs)
            csv_writer.writerow(["Field", "Data Type", "Occurrence", "Top 10 Values", "Field Count"])
            rows = []
            for key, value in self.schema.items():
                # 获取前10个最常见的值
                top_values = []
                if "values" in value:
                    sorted_values = sorted(value["values"].items(), key=lambda x: x[1], reverse=True)
                    for val, count in sorted_values[:10]:
                        top_values.append(f"{val}({count})")
                top_values_str = ", ".join(top_values) if top_values else "N/A"
                
                # 使用字段的出现次数
                field_count = value["sum"]
                
                rows.append([
                    key,
                    value["type"],
                    value["occurrence"],
                    top_values_str,
                    field_count
                ])
            for row in rows:
                csv_writer.writerow(row)

    def __str__(self, out="ascii"):
        """Printable representation of the schema."""
        if not self.schema:
            self.analyze()
        # Prepare the ASCII table
        table = Texttable()
        table.set_cols_align(['l', 'l', 'l', 'l', 'l'])
        table.set_cols_valign(['m', 'm', 'm', 'm', 'm'])
        table.set_cols_dtype(['t', 'i', 'a', 't', 'i'])
        table.add_row(["Field", "Data Type", "Occurrence", "Top 10 Values", "Field Count"])
        # Create the rows
        rows = []
        for key, value in self.schema.items():
            # 获取前10个最常见的值
            top_values = []
            if "values" in value:
                sorted_values = sorted(value["values"].items(), key=lambda x: x[1], reverse=True)
                for val, count in sorted_values[:10]:
                    top_values.append(f"{val}({count})")
            top_values_str = ", ".join(top_values) if top_values else "N/A"
            
            # 使用字段的出现次数
            field_count = value["sum"]
            
            rows.append([
                key,
                value["type"],
                value["occurrence"],
                top_values_str,
                field_count
            ])
        for row in rows:
            table.add_row(row)
        return table.draw() + '\n'
