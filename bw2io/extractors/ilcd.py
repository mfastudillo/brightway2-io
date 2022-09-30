import zipfile
from pathlib import Path
from typing import Union

from lxml import etree

# Xpath for values in process XML file will return one value in a list
xpaths_process = {
    "basename": "/processDataSet/processInformation/dataSetInformation/name/baseName/text()",
    "treatment_standards_routes": "/processDataSet/processInformation/dataSetInformation/name/treatmentStandardsRoutes/text()",
    "mix_and_location_types": "/processDataSet/processInformation/dataSetInformation/name/mixAndLocationTypes/text()",
    "functional_unit_flow_properties": "/processDataSet/processInformation/dataSetInformation/name/functionalUnitFlowProperties/text()",
    "uuid": "/processDataSet/processInformation/dataSetInformation/common:UUID/text()",
    "reference_year": "/processDataSet/processInformation/time/common:referenceYear/text()",
    "data_set_valid_until": "/processDataSet/processInformation/time/common:dataSetValidUntil/text()",
    "location": "/processDataSet/processInformation/geography/locationOfOperationSupplyOrProduction/@location",
    "reference_to_reference_flow": "/processDataSet/processInformation/quantitativeReference/referenceToReferenceFlow/text()",
    # Xpath for values in process XML file, will return multiple values as a list
    "exchanges_internal_id": "/processDataSet/exchanges/exchange/@dataSetInternalID",
    "exchanges_name": "/processDataSet/exchanges/exchange/referenceToFlowDataSet/common:shortDescription/text()",
    "exchanges_uuid": "/processDataSet/exchanges/exchange/referenceToFlowDataSet/@refObjectId",
    "exchanges_direction": "/processDataSet/exchanges/exchange/exchangeDirection/text()",
    "exchanges_amount": "/processDataSet/exchanges/exchange/resultingAmount/text()",
}

# Xpath for values in flow XML files, will return one values in a list
xpaths_flows = {
    "basename": "/flowDataSet/flowInformation/dataSetInformation/name/baseName/text()",
    "uuid": "/flowDataSet/flowInformation/dataSetInformation/common:UUID/text()",
    "category": "/flowDataSet/flowInformation/dataSetInformation/classificationInformation/common:elementaryFlowCategorization/common:category[@level=2]/text()",
    "type": "/flowDataSet/modellingAndValidation/LCIMethod/typeOfDataSet/text()",
    "value": "/flowDataSet/flowProperties/flowProperty[@dataSetInternalID=/flowDataSet/flowInformation/quantitativeReference/referenceToReferenceFlowProperty/text()]/meanValue/text()",
    "refobj": "/flowDataSet/flowProperties/flowProperty[@dataSetInternalID=/flowDataSet/flowInformation/quantitativeReference/referenceToReferenceFlowProperty/text()]/referenceToFlowPropertyDataSet/@refObjectId",
}

# Namespaces to use with the XPath
namespaces = {
    "default_process_ns": {"pns": "http://lca.jrc.it/ILCD/Process"},
    "default_flow_ns": {"fns": "http://lca.jrc.it/ILCD/Flow"},
    "others": {"common": "http://lca.jrc.it/ILCD/Common"},
}


def extract_zip(path: Union[Path, str] = None):
    # ILCD should be read in a particular order
    sort_order = {
        "contacts": 0,
        "sources": 1,
        "unitgroups": 2,
        "flowproperties": 3,
        "flows": 4,
        "processes": 5,
        "external_docs": 6,
    }

    # for the moment we ignore some of the folders
    to_ignore = [
        "contacts",
        "sources",
        "unitgroups",
        "flowproperties",
        "external_docs",
        # "processes",
    ]

    if path is None:
        path = (
            Path(__file__).parent.parent
            / "data"
            / "examples"
            / "ilcd_example.zip"
        )
    with zipfile.ZipFile(path, mode="r") as archive:
        filelist = archive.filelist

        # remove folders that we do not need
        filelist = [
            file
            for file in filelist
            if Path(file.filename).parts[1] not in to_ignore
        ]

        # sort by folder
        filelist = sorted(
            filelist, key=lambda x: sort_order.get(Path(x.filename).parts[1])
        )

        trees = {}
        for file in filelist:
            file_type = Path(file.filename).parts[1]
            if file_type not in trees:
                trees[file_type] = {}
            f = archive.read(file)
            trees[file_type][file.filename] = etree.fromstring(f)

    return trees

def extract_all_relevant_info(file_path):
    trees = extract_zip(file_path)
    file_types = ['processes','flows']
    results = {}
    for ft in file_types:
        results[ft] = []
        for exc_f in trees[ft]:
            tree_object = trees[ft][exc_f]
            if ft == 'processes':
                results[ft].append(apply_xpaths_to_xml_file(xpaths_process, tree_object))
            if ft == 'flows':
                results[ft].append(apply_xpaths_to_xml_file(xpaths_flows, tree_object))
    return results

def apply_xpaths_to_xml_file(xpath_dict, xml_tree):
    results = {}
    for k in xpath_dict:
        default_ns = namespaces["default_process_ns"] if "flowDataSet" not in list(xpath_dict.items())[0][1] else namespaces["default_flow_ns"]
        results[k] = get_xml_value(
            xml_tree, xpath_dict[k], default_ns, namespaces["others"])
    return results


def get_xml_value(xml_tree, xpath_str, default_ns, namespaces):
    assert (
        len(default_ns) == 1
    ), "The general namespace is not clearly defined."
    namespaces.update(default_ns)

    # Adding the general namespace name to xpath expression
    xpath_segments = xpath_str.split("/")
    namespace_abbrevation = list(default_ns.keys())[0]
    for i in range(len(xpath_segments)):
        if (
            ":" not in xpath_segments[i]
            and "(" not in xpath_segments[i]
            and "@" not in xpath_segments[i][:1]
            and "" != xpath_segments[i]
        ):
            xpath_segments[i] = namespace_abbrevation + ":" + xpath_segments[i]
    xpath_str = "/".join(xpath_segments)
    r = xml_tree.xpath(xpath_str, namespaces=namespaces)
    #assert len(r) <= 1, "Unexpected results from XML parsing: " + xpath_str + ", " + str(len(r))
    if len(r)==0: return None
    if len(r)==1: return r[0]
    return r


# looks up the name and unit of a flow property by its UUID 
# to get the unit: lookup_flowproperty('93a60a56-a3c8-11da-a746-0800200c9a66')[0]
# to get the name of the flowproperty: lookup_flowproperty('93a60a56-a3c8-11da-a746-0800200c9a66')[1]
def lookup_flowproperty(flowproperty_uuid):
    fp_dict = {'93a60a56-a3c8-19da-a746-0800200c9a66': ('m2', 'Area'),
    '93a60a56-a3c8-21da-a746-0800200c9a66': ('m2a', 'Areatime'),
    '93a60a56-a3c8-14da-a746-0800200c9a66': ('MJ', 'Gross calorific value'),
    '93a60a56-a3c8-11da-a746-0800200c9a66': ('MJ', 'Net calorific value'),
    'f65d356a-d702-4d79-850e-dd68b47bbcd9': ('Item(s)',
    'Comparative Toxic Unit for human (CTUh)'),
    '585d3441-af58-49c9-a5c2-1d1e5b63f8d5': ('Item(s)',
    'Comparative Toxic Unit for ecosystems (CTUe)'),
    '01846770-4cfe-4a25-8ad9-919d8d378345': ('Item(s)', 'Number of items'),
    'fec15e58-109a-4775-bc6f-72ca0b9b3cee': ('kg', 'Carbon content'),
    'fa94ba11-2720-441d-b432-a2774f3ab1ad': ('kg', 'Gold content'),
    'f3e1017d-1aa3-4763-b563-9494858e1ec7': ('kg', 'Hydrogen content (mass)'),
    'f33b22c4-e0e7-4fa6-ae2b-246ecb0a16eb': ('kg', 'Nickel content'),
    'f2149ae0-b105-4299-b1f1-cf1fd1577867': ('kg', 'Chromium content'),
    'f18a0b1c-59ec-4c76-ae20-bc6c522547b3': ('kg', 'Tellurium content'),
    'e8b9419c-c36e-445f-a3a9-ace79156ac30': ('kg', 'Erbium content'),
    'e57088b8-0313-45c4-b791-6fdfbb59a142': ('kg', 'Samarium content'),
    'e4225227-7855-41ce-ba57-a5314fdcdcf1': ('kg', 'Gadolinium content'),
    'e1b01126-0456-41b5-a29a-d6bd5095b64b': ('kg', 'Sodium content'),
    'e02ff80b-c3f1-4d9d-b33a-f8c3001c70b8': ('kg', 'Rhodium content'),
    'dd77d131-c2ee-4d5a-b5d3-f7aa981ae305': ('kg', 'Scandium content'),
    'dccec68c-c1c9-42e4-9fa9-42dbb943082d': ('kg', 'Cadmium content'),
    'd7882bb5-cb4e-4ef6-84d8-1e1b75de022c': ('kg', 'Neon content (mass)'),
    'd703ed04-22ca-4151-85ae-da1fcf54df68': ('kg', 'Niobium content'),
    'd6b6a36c-58b3-48c7-b723-392a6df4754f': ('kg', 'Aluminium content'),
    'd58f03c0-feab-4e47-aef6-4394aecc72dd': ('kg', 'Lutetium content'),
    'd55035e1-5a9a-44c5-88ce-d20086ab7148': ('kg', 'Neodymium content'),
    'd4f76a48-d1c9-4c49-a46e-b61feb974ac4': ('kg', 'Bromine content'),
    'd07d61ac-df46-4e74-bd49-3260c7b5fb89': ('kg', 'Proactinium content'),
    'cefa9508-86b9-4fad-824e-bbf9d6fdd6f9': ('kg', 'Arsenic content'),
    'c2a5d679-93be-44a1-b82f-da986e622ab9': ('kg', 'Europium content'),
    'bd9b36ca-8240-464a-bba0-01c5210a5b66': ('kg', 'Magnesium content'),
    'b8ef5a2b-ed75-4497-a061-1ecbbd79a380': ('kg', 'Uranium content'),
    'b8a29b5f-1e7e-4f15-881f-e802e42551eb': ('kg', 'Thallium content'),
    'b7be6dba-97f0-42f6-b173-e2c6b042fbe7': ('kg', 'Sulfur content'),
    'b539c404-ce19-43fb-ba14-6a1bdd46d8be': ('kg', 'Praseodym content'),
    'b1dd578a-f16a-4eaa-ae08-442aa2a1a2de': ('kg', 'Helium content (mass)'),
    'b1192e5a-4a70-4fd7-89a2-6b798c7ed3a7': ('kg', 'Cobalt content'),
    'ac557972-7563-49fb-8461-29a4fe8600b7': ('kg', 'Gallium content'),
    'ab654e01-766b-411b-8902-cd6a9c40e55e': ('kg', 'Palladium content'),
    'aaa95528-e05d-4eb4-85ff-a5445b580dcc': ('kg', 'Oxygen content (mass)'),
    'a5278d68-2936-477b-b9de-7ad2d66422ec': ('kg', 'Strontium content'),
    'a0f29f45-108a-4aef-8a66-aef07ed34913': ('kg', 'Iron content'),
    '9bb58775-7ba8-41a7-8818-f75af4f5fe39': ('kg', 'Platinum content'),
    '98eec5a0-44db-411a-a502-95139090bc61': ('kg', 'Iridium content'),
    '984505b9-8536-4e31-a312-0d1a3788f009': ('kg', 'Chlorine content (mass)'),
    '93a60a56-a3c8-11da-a746-0800200b9a66': ('kg', 'Mass'),
    '935e0cbe-3369-4537-8019-4157cf929304': ('kg', 'Terbium content'),
    '92f2d417-544a-4fd1-bde7-25967ef93586': ('kg', 'Molybdenum content'),
    '85aa9476-64fb-4c86-bc01-0c656b479cb8': ('kg', 'Lithium content'),
    '83c2eede-677e-43ec-98c8-1d801c7f1774': ('kg', 'Xenon content'),
    '83040211-0076-4862-b172-79ce62b71187': ('kg', 'Beryllium content'),
    '827934c0-561e-402c-82f2-f04765d180e5': ('kg', 'Bismuth content'),
    '825ef2f3-f706-4c47-8f8d-ee9ddbab3b43': ('kg', 'Tungsten content'),
    '7ec8d70e-4ffc-4024-86b7-6141cc0a2bf5': ('kg', 'Water content (mass)'),
    '7eb6e3cb-0c12-4216-99b5-2032906d42ca': ('kg', 'Lanthanum content'),
    '7eb51190-01f0-40a0-801d-b5c588272314': ('kg', 'Krypton content (mass)'),
    '7cc6f031-f747-49e0-9600-ce2a2fe5657e': ('kg', 'Vanadium content'),
    '7c367337-54f2-4fe9-8ee3-3f42681b0792': ('kg', 'Zinc content'),
    '78b712c9-7f07-467f-b085-efee6067871b': ('kg', 'Osmium content')}
    return fp_dict.get(flowproperty_uuid)
