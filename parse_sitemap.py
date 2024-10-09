import json
import xml.etree.ElementTree as ET
import re


def convert_to_mb(size, units):
    size = float(size)
    if units.lower() == 'gbytes':
        return str(size * 1024)
    return str(size)


def parse_xml_file(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    ns = {'catalog': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}

    data = {}

    for dataset in root.findall('.//catalog:dataset', ns):
        name = dataset.get('name')
        if name and name.endswith('.nc'):
            match = re.match(r'(.+)\.(\d{6})\.nc', name)
            if match:
                variable, date = match.groups()
                size_element = dataset.find('catalog:dataSize', ns)
                if size_element is not None:
                    size = size_element.text
                    units = size_element.get('units')
                    size_mb = convert_to_mb(size, units)

                    if variable not in data:
                        data[variable] = {}
                    data[variable][date] = size_mb

    return data


def process_narr_data(file_path):
    years = range(1979, 2025)
    months = range(1, 13)

    data = parse_xml_file(file_path)

    for variable in data:
        for year in years:
            for month in months:
                date = f"{year}{month:02d}"
                if date not in data[variable]:
                    data[variable][date] = "N/A"

    return data


file_path = 'pressure_sitemap.xml'
result = process_narr_data(file_path)


print("Sample of the parsed data:")
for variable in list(result.keys())[:2]:
    print(f"{variable}:")
    print(dict(list(result[variable].items())[:5]))
    print("...")


with open('pressure_sizes.json', 'w') as f:
    json.dump(result, f, indent=2)
