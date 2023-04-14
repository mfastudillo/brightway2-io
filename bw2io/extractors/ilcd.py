import zipfile
from pathlib import Path
from typing import Union
import pandas as pd

from lxml import etree

def extract_(path_to_zip) -> list:
    """parse a zip file with the ilcd dataset

    Args:
        path_to_zip (_type_): _description_

    Returns:
        list: list of dicts with the extracted data from the xml files. Each
        element of the list represents an activity.
    """

    etrees_dict = extract_zip(path_to_zip)

    #get product system model if exists
    if 'lifecyclemodels' in etrees_dict:
        psm = get_systemmodel(etrees_dict)

    # get contanct data
    contact_list = get_contact_from_etree(etrees_dict)

    # get unit group data 
    unit_gr_dict = get_unitdata_from_etree(etree_dict=etrees_dict)

    # get flow properties
    flow_properties_list = get_flowproperties_from_etree(etrees_dict)
    
    # combine the flow property and unit data
    unit_fp_dict = fp_dict(flow_properties=flow_properties_list, ug_dict=unit_gr_dict)
    
    # extract more info from `flows` folder
    flow_list = get_flows_from_etree(etrees_dict)

    flow_df = pd.DataFrame(flow_list)


    # get activity data and first part of exchanges
    activity_list = get_activity_from_etree(etrees_dict)
    
    activity_info_list = []
    for activity_info,exchange_dict in activity_list:
        # this merging could be also done without pandas 
        try:
            df_exchages = pd.DataFrame(exchange_dict)
        except ValueError as ve:
            # if it is only one
            df_exchages = pd.Series(exchange_dict,name=0).to_frame().T
            assert len(df_exchages) == 1,'we may be under the wrong assumption'

        exchanges = flow_df.merge(
            df_exchages, left_on="uuid", right_on="exchanges_uuid", how="inner"
        ).drop("exchanges_uuid", axis=1)

        exchanges["amount"] = exchanges["exchanges_amount"].map(float) * exchanges[
            "value"
        ].map(float)

        exchanges = exchanges.drop(["value", "exchanges_amount"], axis=1)

        # add unit and flow property from lookup
        # unit_fp_df_v0 = pd.DataFrame(
        #     exchanges.refobj.map(lookup_flowproperty).to_list(),
        #     index=exchanges.index,
        #     columns=["unit", "flow property"],
        # )
        # # new approach instead of lookup
        missing_keys = set(exchanges.refobj).difference(set(unit_fp_dict))
        assert len(missing_keys) == 0,f"these keys are missing a unit and flow prop {missing_keys}"

        unit_fp_df = pd.DataFrame(exchanges.refobj.map(unit_fp_dict).to_list(),
        index=exchanges.index)

        # # validation, the new way is equivalent to lookup
        # comparison = unit_fp_df_v0.compare(unit_fp_df[unit_fp_df_v0.columns],
        # result_names=('v0','v1'),align_axis=0)
        # #assert len(comparison) == 0,comparison

        exchanges[unit_fp_df.columns] = unit_fp_df

        activity_info["exchanges"] = exchanges.to_dict("records")
        activity_info["contacts"] = contact_list
        activity_info["flow properties"] = flow_properties_list
        # activity_info['_unit_group'] = unit_gr_list
        activity_info['_unit_flow_prop'] = unit_fp_dict # used later for unit conv
        activity_info_list.append(activity_info)

    return activity_info_list

def xpaths()-> dict:
    """_summary_

    Returns:
        dict: xpaths related to the different folders
    """
    # Xpath for values in process XML file will return one value in a list
    xpaths_process = {
    # process information
    "basename": "/processDataSet/processInformation/dataSetInformation/name/baseName/text()",
    "treatment_standards_routes": "/processDataSet/processInformation/dataSetInformation/name/treatmentStandardsRoutes/text()",
    "mix_and_location_types": "/processDataSet/processInformation/dataSetInformation/name/mixAndLocationTypes/text()",
    "functional_unit_flow_properties": "/processDataSet/processInformation/dataSetInformation/name/functionalUnitFlowProperties/text()",
    "uuid": "/processDataSet/processInformation/dataSetInformation/common:UUID/text()",
    "general_comment":"/processDataSet/processInformation/dataSetInformation/common:generalComment/text()",
    "reference_year": "/processDataSet/processInformation/time/common:referenceYear/text()",
    "data_set_valid_until": "/processDataSet/processInformation/time/common:dataSetValidUntil/text()",
    "time_representativeness_description": "/processDataSet/processInformation/time/common:timeRepresentativenessDescription/text()",
    "location": "/processDataSet/processInformation/geography/locationOfOperationSupplyOrProduction/@location",
    "LatLong": "/processDataSet/processInformation/geography/locationOfOperationSupplyOrProduction/@latitudeAndLongitude",
    "reference_to_reference_flow": "/processDataSet/processInformation/quantitativeReference/referenceToReferenceFlow/text()",
    # Xpath for values in process XML file, will return multiple values as a list
    "parameter_name":"/processDataSet/processInformation/mathematicalRelations/variableParameter/@name",
    "parameter_comment":"/processDataSet/processInformation/mathematicalRelations/variableParameter/comment/text()",
    "parameter_mean_value":"/processDataSet/processInformation/mathematicalRelations/variableParameter/meanValue/text()",
    "parameter_minimum_value":"/processDataSet/processInformation/mathematicalRelations/variableParameter/minimumValue/text()",
    "parameter_maximum_value":"/processDataSet/processInformation/mathematicalRelations/variableParameter/maximumValue/text()",
    "parameter_std95":"/processDataSet/processInformation/mathematicalRelations/variableParameter/relativeStandardDeviation95In/text()",
    "parameter_formula":"/processDataSet/processInformation/mathematicalRelations/variableParameter/formula/text()",
    "parameter_distrib":"/processDataSet/processInformation/mathematicalRelations/variableParameter/uncertaintyDistributionType/text()",

    # administrative info
    'intended_application':"/processDataSet/administrativeInformation/common:commissionerAndGoal/common:intendedApplications/text()",
    'dataset_format':"/processDataSet/administrativeInformation/dataEntryBy/common:referenceToDataSetFormat/common:shortDescription/text()",
    "licensetype":"/processDataSet/administrativeInformation/publicationAndOwnership/common:licenseType/text()",
    # exchanges (we start exchange data by exchange_ to parse it later )
    "exchanges_internal_id": "/processDataSet/exchanges/exchange/@dataSetInternalID",
    "exchanges_name": "/processDataSet/exchanges/exchange/referenceToFlowDataSet/common:shortDescription/text()",
    "exchanges_uuid": "/processDataSet/exchanges/exchange/referenceToFlowDataSet/@refObjectId",
    "exchanges_direction": "/processDataSet/exchanges/exchange/exchangeDirection/text()",
    "exchanges_amount": "/processDataSet/exchanges/exchange/resultingAmount/text()",
    "exchanges_amount_min": "/processDataSet/exchanges/exchange/minimumAmount/text()",
    "exchanges_amount_max": "/processDataSet/exchanges/exchange/maximumAmount/text()",
    "exchanges_amount_distrib": "/processDataSet/exchanges/exchange/uncertaintyDistributionType/text()",
#    "exchanges_param":"/processDataSet/exchanges/exchange/referenceToVariable/text()",
    }

    xpaths_exchanges = {
    "exchanges_name": "/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/referenceToFlowDataSet/common:shortDescription/text()",
    "exchanges_uuid": "/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/referenceToFlowDataSet/@refObjectId",
    'flow_uuid':"/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/referenceToFlowDataSet/@refObjectId",
    'exchanges_amount':"/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/resultingAmount/text()",
    'exchanges_param_name':"/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/referenceToVariable/text()",
    "exchanges_amount_min": "/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/minimumAmount/text()",
    "exchanges_amount_max": "/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/maximumAmount/text()",
    "exchanges_amount_distrib": "/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/uncertaintyDistributionType/text()",
    }

    # Xpath for values in flow XML files, will return one values in a list
    internal_id = '/flowDataSet/flowInformation/quantitativeReference/referenceToReferenceFlowProperty/text()'
    xpaths_flows = {
    # flowinformation
    "basename": "/flowDataSet/flowInformation/dataSetInformation/name/baseName/text()",
    "uuid": "/flowDataSet/flowInformation/dataSetInformation/common:UUID/text()",
    "category_0": "/flowDataSet/flowInformation/dataSetInformation/classificationInformation/common:elementaryFlowCategorization/common:category[@level=0]/text()",
    "category_1": "/flowDataSet/flowInformation/dataSetInformation/classificationInformation/common:elementaryFlowCategorization/common:category[@level=1]/text()",
    "category_2": "/flowDataSet/flowInformation/dataSetInformation/classificationInformation/common:elementaryFlowCategorization/common:category[@level=2]/text()",
    'CAS number':"/flowDataSet/flowInformation/dataSetInformation/CASNumber/text()",
    # modelling and validation
    "type": "/flowDataSet/modellingAndValidation/LCIMethod/typeOfDataSet/text()",
    "value": f"/flowDataSet/flowProperties/flowProperty[@dataSetInternalID={internal_id}]/meanValue/text()",
    # flow properties
    "refobj": f"/flowDataSet/flowProperties/flowProperty[@dataSetInternalID={internal_id}]/referenceToFlowPropertyDataSet/@refObjectId",
    "flow property description":f"/flowDataSet/flowProperties/flowProperty[@dataSetInternalID={internal_id}]/referenceToFlowPropertyDataSet/common:shortDescription/text()",
    }

    xpath_contacts = {
    'email':"/contactDataSet/contactInformation/dataSetInformation/email/text()",
    'website':"/contactDataSet/contactInformation/dataSetInformation/WWWAddress/text()",
    'short_name':"/contactDataSet/contactInformation/dataSetInformation/common:shortName/text()",
    }
    xpath_flowproperties = {
    'flow_property_name':"/flowPropertyDataSet/flowPropertiesInformation/dataSetInformation/common:name[@xml:lang='en']/text()", # only the english one
    'refObjectId_unitgroup':'/flowPropertyDataSet/flowPropertiesInformation/quantitativeReference/referenceToReferenceUnitGroup/@refObjectId',
    "refobjuuid":'/flowPropertyDataSet/flowPropertiesInformation/dataSetInformation/common:UUID/text()'
    }
    
    unit_internal_id = '/unitGroupDataSet/unitGroupInformation/quantitativeReference/referenceToReferenceUnit/text()'
    xpath_unitgroups = {
    'ref_to_refunit':unit_internal_id,
    'ug_uuid':'/unitGroupDataSet/unitGroupInformation/dataSetInformation/common:UUID/text()',
    'unit_name':f'/unitGroupDataSet/units/unit[@dataSetInternalID={unit_internal_id}]/name/text()',
    'unit_amount':f'/unitGroupDataSet/units/unit[@dataSetInternalID={unit_internal_id}]/meanValue/text()',
    'ug_name':"/unitGroupDataSet/unitGroupInformation/dataSetInformation/common:name[@xml:lang='en']/text()",
    }

    xpaths_lifecyclemodel = {
    'ref_to_refproc':"/lifeCycleModelDataSet/lifeCycleModelInformation/quantitativeReference/referenceToReferenceProcess/text()",
    'internal_ids':"/lifeCycleModelDataSet/lifeCycleModelInformation/technology/processes/processInstance/@dataSetInternalID",
    'downstream_bruto':"/lifeCycleModelDataSet/lifeCycleModelInformation/technology/processes/processInstance/connections/outputExchange/downstreamProcess/@id",
    }

    xpaths_dict = {'xpath_contacts':xpath_contacts,'xpaths_flows':xpaths_flows,
    'xpaths_process':xpaths_process,'xpath_flowproperties':xpath_flowproperties,
    'xpaths_unitgroups':xpath_unitgroups,'xpaths_exchanges':xpaths_exchanges,
    'xpaths_lifecyclemodel':xpaths_lifecyclemodel}

    return xpaths_dict


def namespaces_dict()-> dict:
    # Namespaces to use with the XPath (from files under xmlns)
    namespaces = {
        "default_process_ns": {"pns": "http://lca.jrc.it/ILCD/Process"},
        "default_flow_ns": {"fns": "http://lca.jrc.it/ILCD/Flow"},
        "others": {"common": "http://lca.jrc.it/ILCD/Common"},
        'default_contact_ns': {"contact":"http://lca.jrc.it/ILCD/Contact"},
        "default_fp_ns":{'fpns':'http://lca.jrc.it/ILCD/FlowProperty'},
        "default_unitgroup_ns":{'ugns':'http://lca.jrc.it/ILCD/UnitGroup'},
        "lifecyclemodel_ns":{'lcmns':'http://eplca.jrc.ec.europa.eu/ILCD/LifeCycleModel/2017'}
    }

    return namespaces

def extract_zip(path: Union[Path, str] = None)-> dict:
    """

    Args:
        path (Union[Path, str], optional): _description_. Defaults to None.

    Returns:
        dict: _description_
    """
    # ILCD should be read in a particular order
    sort_order = {
        "contacts": 0,
        "sources": 1,
        "unitgroups": 2,
        "flowproperties": 3,
        "flows": 4,
        "processes": 5,
        "external_docs": 6,
        "lifecyclemodels":7,
    }

    # for the moment we ignore some of the folders
    to_ignore = [
        "sources",
        "external_docs",
    ]

    with zipfile.ZipFile(path, mode="r") as archive:
        filelist = archive.filelist

        # filter dirs, only files, sometimes there is empty folders there
        filelist = [f for f in filelist if f.is_dir() == False]

        # remove folders that we do not need
        filelist = [
        file for file in filelist if Path(file.filename).parent.name not in to_ignore
         ]

        # remove non xml files
        filelist = [
        file for file in filelist if Path(file.filename).suffix == '.xml'
                ]


        # sort by folder (a default key for folders that go at the end wo order)
        filelist = sorted(
            filelist, key=lambda x: sort_order.get(Path(x.filename).parts[1],99)
        )

        trees = {}
        for file in filelist:
            file_type = Path(file.filename).parts[1]
            if file_type not in trees:
                trees[file_type] = {}
            f = archive.read(file)
            trees[file_type][file.filename] = etree.fromstring(f)

    return trees


def extract_all_relevant_info(file_path: Union[Path, str])-> dict:
    """extracts all the relevant info from a zip file containing a lci in ilcd 
    format (DEPRECATED)

    Args:
        file_path (Union[Path, str]): path to the file bein extracted

    Returns:
        dict: contains relevant info
    """
    xpaths_dict = xpaths()
    xpaths_process = xpaths_dict['xpaths_process']
    xpaths_flows = xpaths_dict['xpaths_flows']
    xpath_contacts = xpaths_dict['xpath_contacts']

    trees = extract_zip(file_path)
    file_types = ["contacts","processes", "flows",'flowproperties']
    results = {}
    for ft in file_types:
        results[ft] = []
        for exc_f in trees[ft]:
            tree_object = trees[ft][exc_f]

            if ft == "processes":
                results[ft].append(
                    apply_xpaths_to_xml_file(xpaths_process, tree_object)
                )
            elif ft == "flows":
                results[ft].append(apply_xpaths_to_xml_file(xpaths_flows, tree_object))
            elif ft == 'contacts':
                results[ft].append(apply_xpaths_to_xml_file(xpath_contacts,tree_object))
            elif ft == 'flowproperties':
                results[ft].append(apply_xpaths_to_xml_file(xpaths_dict['xpath_flowproperties'],tree_object))

    return results


def apply_xpaths_to_xml_file(xpath_dict:dict, xml_tree)-> dict:
    """_summary_

    Args:
        xpath_dict (dict): _description_
        xml_tree (_type_): _description_

    Returns:
        dict: _description_
    """
    namespaces = namespaces_dict()

    results = {}
    hint = list(xpath_dict.items())[0][1].split('/')[1]

    selec_namespace = {
    'contactDataSet':namespaces['default_contact_ns'],
    'flowDataSet':namespaces["default_flow_ns"],
    'processDataSet':namespaces["default_process_ns"],
    "flowPropertyDataSet":namespaces["default_fp_ns"],
    "unitGroupDataSet":namespaces['default_unitgroup_ns'],
    "lifeCycleModelDataSet":namespaces['lifecyclemodel_ns']
    }

    default_ns = selec_namespace[hint]

    for k in xpath_dict:

        results[k] = get_xml_value(
            xml_tree, xpath_dict[k], default_ns, namespaces["others"]
        )
    return results


def get_xml_value(xml_tree, xpath_str, default_ns, namespaces)-> dict:

    assert len(default_ns) == 1, "The general namespace is not clearly defined."
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
    # assert len(r) <= 1, "Unexpected results from XML parsing: " + xpath_str + ", " + str(len(r))
    if len(r) == 0:
        return None
    if len(r) == 1:
        return r[0]
    return r


def lookup_flowproperty(flowproperty_uuid:str)-> tuple:
    """(deprecated)

    Args:
        flowproperty_uuid (str): unique identifier of flow property

    Returns:
        tuple: the unit and flow property of a given flow given its uuid
    """
    # TODO: this dictionary seems to be possible to construct looking into the
    # flowproperties xml files for the uuid and name and .. somewhere else. 
    # insane

    # TODO: the flow property seems to be extracted now, so we can use this only
    # for the unit

    fp_dict = {
        "93a60a56-a3c8-19da-a746-0800200c9a66": ("m2", "Area"),
        "93a60a56-a3c8-21da-a746-0800200c9a66": ("m2*a", "Area*time"),
        "93a60a56-a3c8-14da-a746-0800200c9a66": ("MJ", "Gross calorific value"),
        "93a60a56-a3c8-11da-a746-0800200c9a66": ("MJ", "Net calorific value"),
        "f65d356a-d702-4d79-850e-dd68b47bbcd9": (
            "Item(s)",
            "Comparative Toxic Unit for human (CTUh)",
        ),
        "585d3441-af58-49c9-a5c2-1d1e5b63f8d5": (
            "Item(s)",
            "Comparative Toxic Unit for ecosystems (CTUe)",
        ),
        "01846770-4cfe-4a25-8ad9-919d8d378345": ("Item(s)", "Number of items"),
        "fec15e58-109a-4775-bc6f-72ca0b9b3cee": ("kg", "Carbon content"),
        "fa94ba11-2720-441d-b432-a2774f3ab1ad": ("kg", "Gold content"),
        "f3e1017d-1aa3-4763-b563-9494858e1ec7": ("kg", "Hydrogen content (mass)"),
        "f33b22c4-e0e7-4fa6-ae2b-246ecb0a16eb": ("kg", "Nickel content"),
        "f2149ae0-b105-4299-b1f1-cf1fd1577867": ("kg", "Chromium content"),
        "f18a0b1c-59ec-4c76-ae20-bc6c522547b3": ("kg", "Tellurium content"),
        "e8b9419c-c36e-445f-a3a9-ace79156ac30": ("kg", "Erbium content"),
        "e57088b8-0313-45c4-b791-6fdfbb59a142": ("kg", "Samarium content"),
        "e4225227-7855-41ce-ba57-a5314fdcdcf1": ("kg", "Gadolinium content"),
        "e1b01126-0456-41b5-a29a-d6bd5095b64b": ("kg", "Sodium content"),
        "e02ff80b-c3f1-4d9d-b33a-f8c3001c70b8": ("kg", "Rhodium content"),
        "dd77d131-c2ee-4d5a-b5d3-f7aa981ae305": ("kg", "Scandium content"),
        "dccec68c-c1c9-42e4-9fa9-42dbb943082d": ("kg", "Cadmium content"),
        "d7882bb5-cb4e-4ef6-84d8-1e1b75de022c": ("kg", "Neon content (mass)"),
        "d703ed04-22ca-4151-85ae-da1fcf54df68": ("kg", "Niobium content"),
        "d6b6a36c-58b3-48c7-b723-392a6df4754f": ("kg", "Aluminium content"),
        "d58f03c0-feab-4e47-aef6-4394aecc72dd": ("kg", "Lutetium content"),
        "d55035e1-5a9a-44c5-88ce-d20086ab7148": ("kg", "Neodymium content"),
        "d4f76a48-d1c9-4c49-a46e-b61feb974ac4": ("kg", "Bromine content"),
        "d07d61ac-df46-4e74-bd49-3260c7b5fb89": ("kg", "Proactinium content"),
        "cefa9508-86b9-4fad-824e-bbf9d6fdd6f9": ("kg", "Arsenic content"),
        "c2a5d679-93be-44a1-b82f-da986e622ab9": ("kg", "Europium content"),
        "bd9b36ca-8240-464a-bba0-01c5210a5b66": ("kg", "Magnesium content"),
        "b8ef5a2b-ed75-4497-a061-1ecbbd79a380": ("kg", "Uranium content"),
        "b8a29b5f-1e7e-4f15-881f-e802e42551eb": ("kg", "Thallium content"),
        "b7be6dba-97f0-42f6-b173-e2c6b042fbe7": ("kg", "Sulfur content"),
        "b539c404-ce19-43fb-ba14-6a1bdd46d8be": ("kg", "Praseodym content"),
        "b1dd578a-f16a-4eaa-ae08-442aa2a1a2de": ("kg", "Helium content (mass)"),
        "b1192e5a-4a70-4fd7-89a2-6b798c7ed3a7": ("kg", "Cobalt content"),
        "ac557972-7563-49fb-8461-29a4fe8600b7": ("kg", "Gallium content"),
        "ab654e01-766b-411b-8902-cd6a9c40e55e": ("kg", "Palladium content"),
        "aaa95528-e05d-4eb4-85ff-a5445b580dcc": ("kg", "Oxygen content (mass)"),
        "a5278d68-2936-477b-b9de-7ad2d66422ec": ("kg", "Strontium content"),
        "a0f29f45-108a-4aef-8a66-aef07ed34913": ("kg", "Iron content"),
        "9bb58775-7ba8-41a7-8818-f75af4f5fe39": ("kg", "Platinum content"),
        "98eec5a0-44db-411a-a502-95139090bc61": ("kg", "Iridium content"),
        "984505b9-8536-4e31-a312-0d1a3788f009": ("kg", "Chlorine content (mass)"),
        "93a60a56-a3c8-11da-a746-0800200b9a66": ("kg", "Mass"),
        "935e0cbe-3369-4537-8019-4157cf929304": ("kg", "Terbium content"),
        "92f2d417-544a-4fd1-bde7-25967ef93586": ("kg", "Molybdenum content"),
        "85aa9476-64fb-4c86-bc01-0c656b479cb8": ("kg", "Lithium content"),
        "83c2eede-677e-43ec-98c8-1d801c7f1774": ("kg", "Xenon content"),
        "83040211-0076-4862-b172-79ce62b71187": ("kg", "Beryllium content"),
        "827934c0-561e-402c-82f2-f04765d180e5": ("kg", "Bismuth content"),
        "825ef2f3-f706-4c47-8f8d-ee9ddbab3b43": ("kg", "Tungsten content"),
        "7ec8d70e-4ffc-4024-86b7-6141cc0a2bf5": ("kg", "Water content (mass)"),
        "7eb6e3cb-0c12-4216-99b5-2032906d42ca": ("kg", "Lanthanum content"),
        "7eb51190-01f0-40a0-801d-b5c588272314": ("kg", "Krypton content (mass)"),
        "7cc6f031-f747-49e0-9600-ce2a2fe5657e": ("kg", "Vanadium content"),
        "7c367337-54f2-4fe9-8ee3-3f42681b0792": ("kg", "Zinc content"),
        "78b712c9-7f07-467f-b085-efee6067871b": ("kg", "Osmium content"),
        "77f40806-2c54-4050-97d3-d1b61eb4b0c9": ("kg", "Boron content"),
        "76d611d0-a0a5-43d5-b391-a4e7ec463052": ("kg", "Calcium content"),
        "74f8559d-44a4-409f-9316-a185882a496c": ("kg", "Radon content"),
        "71fc82ec-9eb9-424a-bc45-053a844c8af9": ("kg", "Selenium content"),
        "699b6a73-e8d5-4755-8d1d-b70de4b9efdd": ("kg", "Phosphorus content"),
        "697fbcb6-b516-4c2f-a78e-5ec2ffaec4b3": ("kg", "Caesium content"),
        "6827435e-8ad9-44c1-b0b4-b26f321957b1": ("kg", "Potassium content"),
        "64b8e725-b0ba-4837-abb9-62dc0e7b9f6f": ("kg", "Carbon content (fossil)"),
        "62e503ce-544a-4599-b2ad-bcea15a7bf20": ("kg", "Carbon content (biogenic)"),
        "61ec59a5-3816-4e5a-88c7-acf8a8b33d41": ("kg", "Silver content"),
        "607a33e0-28ed-42f8-ac8c-66f0c9700e56": ("kg", "Silicon content"),
        "5db5be2d-8df6-432b-ab3d-ffde49d3fe7f": ("kg", "Barium content"),
        "5c39e9a9-4bb5-42cd-b519-0db3a940a575": ("kg", "Ruthenium content"),
        "5a2556bf-a9ec-4720-9253-d40f1de3c2b8": ("kg", "Manganese content"),
        "52dcf2ff-c0f6-417f-a957-788688d67b86": ("kg", "Recycled content (mass)"),
        "476dbccb-c1e5-4cdb-aa14-f4ef627c45ec": ("kg", "Fluorine content (mass)"),
        "435e1d0e-e36e-4d4a-9cd1-7fd2c135da61": ("kg", "Antimony content"),
        "3da87ad0-733e-4ca9-b234-75ef52ca2a2d": ("kg", "Holmium content"),
        "33cf4d9a-bcbf-4fe4-926b-38f2c3e36278": ("kg", "Tantalum content"),
        "2ea4d465-87e3-4f85-81a0-2bd661639f4e": ("kg", "Yttrium content"),
        "2e4695ff-d6a5-42c2-9816-6dbd6c859707": ("kg", "Radium content"),
        "282882cf-a5e5-4170-82dd-8e84da2724d2": ("kg", "Titanium content"),
        "2651cd57-1c24-4b8e-856b-ddb63a3b267e": ("kg", "Mercury content"),
        "25c2946d-2b1f-4245-b2aa-80c037912665": ("kg", "Thorium content"),
        "24a07e87-aa12-4f9c-8c40-580e9bf7a2f2": ("kg", "Zirconium content"),
        "1e6e8252-dae9-4b4a-866c-c6c3a5e810a1": ("kg", "Thulium content"),
        "1d85ab50-ed1e-4421-b3b8-c5197d858a0d": ("kg", "Argon content (mass)"),
        "184c9c12-32fb-4252-a484-9952928b8473": ("kg", "Indium content"),
        "11f31f00-f6fa-4ef0-a9e3-5bd37199dd20": ("kg", "Tin content"),
        "0bb4f3ac-2944-4263-8587-ea6eac2d24ac": ("kg", "Rhenium content"),
        "0b12d18f-0c7c-44da-8c52-dc3278f917c7": ("kg", "Ytterbium content"),
        "0ac18236-1ef4-4d67-95c0-4aebfec7f470": ("kg", "Cerium content"),
        "0ab79a5f-7fd7-4d5d-be55-559292c958d3": ("kg", "Copper content"),
        "085af809-5dc6-4920-aaa1-4ac493ae4af8": ("kg", "Nitrogen content (mass)"),
        "078ecf46-26d6-47e3-8128-fcced884d1cd": ("kg", "Germanium content"),
        "037f9009-8b06-470d-a190-21448f7921c7": ("kg", "Lead content"),
        "02f11c31-4bb8-41de-81fb-fb479f361eb9": ("kg", "Iodine content"),
        "b3f0f892-c5a3-4c66-a432-c09e3d1e9bd6": ("kg*a", "Mass*time"),
        "341fd786-b2ad-4552-a762-5eafcab45dee": ("mol", "Moles"),
        "93a60a56-a3c8-17da-a746-0800200c9a66": ("kBq", "Radioactivity"),
        "fa48378a-140a-4a0a-bf23-2bd64eac7c7c": ("m3", "Methane content (volume)"),
        "93a60a56-a3c8-22da-a746-0800200c9a66": ("m3", "Volume"),
        "441238a3-ba09-46ec-b35b-c30cfba746d1": ("m3*a", "Volume*time"),
    }
    return fp_dict.get(flowproperty_uuid, (None, None))



class ILCDExtractor(object):
    """_summary_

    Args:
        object (_type_): _description_

    Returns:
        _type_: _description_
    """
    @classmethod
    def _extract(cls, path):
        
        assert Path(path).exists(),"path to file does not seem to exist"
        data = extract(path)

        return data

def get_unitdata_from_etree(etree_dict:dict)->dict:
    """extracts data from the unitgroups xml files. for each dataset the uuid in 
    dataset information, and the unit name and multiplier 

    Args:
        etree_dict (dict): _description_

    Returns:
        dict: refobj uuid as key and the name of the unit and multiplier factor
        as values inside a dict
    """
    unit_d = {}

    xpaths_dict = xpaths()
    xpaths_unitgr = xpaths_dict['xpaths_unitgroups']

    for _,etree in etree_dict.get('unitgroups').items():

        unit_gr = apply_xpaths_to_xml_file(xpaths_unitgr,etree)
        
        unit_d[unit_gr['ug_uuid']] = {'unit':unit_gr['unit_name'],
        'multiplier':float(unit_gr['unit_amount']),'unit_group':unit_gr['ug_name']}

    return unit_d

def get_systemmodel(etree_dict:dict)->dict:
    """_summary_

    Parameters
    ----------
    etree_dict : dict
        _description_

    Returns
    -------
    dict
        _description_
    """
    xpaths_dict = xpaths()
    xpaths_psm = xpaths_dict['xpaths_lifecyclemodel']
    
    for _,etree in etree_dict.get('lifecyclemodels').items():

        psm = apply_xpaths_to_xml_file(xpaths_psm,etree)

    # with the internal id we can get the input and output flow connexions
    for internal_id in psm['internal_ids']:

        d = {f'downstream_id_{internal_id}':
        f"/lifeCycleModelDataSet/lifeCycleModelInformation/technology/processes/processInstance[@dataSetInternalID={internal_id}]/connections/outputExchange/downstreamProcess/@id",
        f"downstream_uuid_{internal_id}":
        f"/lifeCycleModelDataSet/lifeCycleModelInformation/technology/processes/processInstance[@dataSetInternalID={internal_id}]/connections/outputExchange/downstreamProcess/@flowUUID",
        f'upstream_uuid_{internal_id}':
        f"/lifeCycleModelDataSet/lifeCycleModelInformation/technology/processes/processInstance[@dataSetInternalID={internal_id}]/connections/outputExchange/@flowUUID",
        }



    return psm

def reorganise_unit_group_data(unit_list):

    ug_dict = {}
    for ug in unit_list:
        
        # transform multipliers to numbers
        ug['unit_amount'] = [float(number) for number in ug['unit_amount']]

        unit_name = ug['unit_name'][int(ug['ref_to_refunit'])]
        unit_amount = float(ug['unit_amount'][int(ug['ref_to_refunit'])])
        ref_unit_name = ug['unit_name'][ug['unit_amount'].index(1)]
    
        ug_dict[ug['ug_uuid']] = {'name':unit_name,'amount':unit_amount,
        'ref_unit':ref_unit_name}

    return ug_dict

def get_contact_from_etree(etree_dict:dict)->list:
    """extracts data from the 'contacts' folder

    Args:
        etree_dict (dict): _description_

    Returns:
        list: list of dicts with contact information
    """
    contact_list = []

    xpaths_dict = xpaths()
    xpath_contacts = xpaths_dict['xpath_contacts']

    for _,etree in etree_dict.get('contacts').items():
        contacts = apply_xpaths_to_xml_file(xpath_contacts,etree)
        contact_list.append(contacts)

    return contact_list


def get_activity_from_etree(etrees_dict:dict)->list:
    """extracts data from the 'processes' folder [DEPRECATED]

    Args:
        etrees_dict (dict): _description_

    Returns:
        list: _description_
    """

    xpaths_dict = xpaths()
    xpaths_process = xpaths_dict['xpaths_process']

    namespaces = namespaces_dict()
    default_ns = namespaces["default_process_ns"]
    ns = namespaces["others"]
    ns.update(default_ns)

    activity_list = []
    for path, etree in etrees_dict["processes"].items():

        activity = apply_xpaths_to_xml_file(xpaths_process, etree)

        # separate exchanges
        exchange_dict = {}
        activity_info = {}
        for key, value in activity.items():

            if key.startswith("exchanges") and value is not None:
                exchange_dict[key] = value
            else:
                activity_info[key] = value

        activity_list.append((activity_info,exchange_dict))
        

    return activity_list
    
def get_flows_from_etree(etrees_dict:dict)->list:
    """extracts data from 'flows' folder

    Parameters
    ----------
    etrees_dict : dict
        _description_

    Returns
    -------
    list
        _description_
    """
    namespaces = namespaces_dict()
    default_ns = namespaces["default_flow_ns"]
    ns = namespaces["others"]
    ns.update(default_ns)

    xpaths_dict = xpaths()
    xpaths_flows = xpaths_dict['xpaths_flows']

    flow_list = []
    for path, etree in etrees_dict["flows"].items():

        thing = apply_xpaths_to_xml_file(xpaths_flows, etree)
        flow_list.append(thing)

    return flow_list


def get_flowproperties_from_etree(etree_dict:dict)->list:
    """extracts data from the 'flowproperties' folder

    Args:
        etree_dict (dict): _description_

    Returns:
        list: _description_
    """
    fp_list = []

    xpaths_dict = xpaths()
    xpath_contacts = xpaths_dict['xpath_flowproperties']

    for _,etree in etree_dict.get('flowproperties').items():
        fp = apply_xpaths_to_xml_file(xpath_contacts,etree)

        # TODO: modify so it returns a list when is just one element
        fp_list.append(fp)

    return fp_list


def fp_dict(flow_properties:list,ug_dict:dict):
    """combines data from the unit group folder and the flow properties folder
    to construct get the unit and the flow property of each exchange using as key
    data from the ... exchanges? folder as well as the multiplier associated with
    a reference unit

    Parameters
    ----------
    flow_properties : list
        _description_
    ug_dict : dict
        _description_

    Returns
    -------
    _type_
        _description_
    """
    fp_dict = {}
    for fp in flow_properties:

        d = {
            'flow property':fp['flow_property_name'],
            'unit':ug_dict[fp['refObjectId_unitgroup']]['unit'],
            'unit_multiplier':ug_dict[fp['refObjectId_unitgroup']]['multiplier'],
            'unit_group':ug_dict[fp['refObjectId_unitgroup']]['unit_group'],
            }
        
        fp_dict[fp['refobjuuid']] = d
    
    return fp_dict

def get_exchanges_ids(etrees_dict)-> dict:
    """the internal exchange ids of the different processes in the ilcd dataset.
    This is later used to better parse the exchanges files

    Parameters
    ----------
    etrees_dict : dict
        _description_

    Returns
    -------
    dict
        uuids of activities as keys and internalids of exchanges as values
    """
    
    reorganised = {}
    for etree in etrees_dict['processes'].values():
        exchanges_internal_ids = apply_xpaths_to_xml_file(
        {'uuid':"/processDataSet/processInformation/dataSetInformation/common:UUID/text()",
        "exchanges_id": "/processDataSet/exchanges/exchange/@dataSetInternalID"},
        etree)

        reorganised[exchanges_internal_ids['uuid']] = exchanges_internal_ids["exchanges_id"]
    
    return reorganised

    
def get_exchange_data(etree_dict):
    """extracts the data on exchanges of the processes files

    Parameters
    ----------
    etree_dict : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    exchanges_internal_ids = get_exchanges_ids(etree_dict)
    d = xpaths()['xpaths_exchanges']

    ex = []

    for path,etree in etree_dict['processes'].items():

        uuid = Path(path).stem[0:36] # trick .. 
        etre_internal_ex_ids = exchanges_internal_ids[uuid]


        exchanges_dict = {}
        for internal_id in etre_internal_ex_ids:
            # formats to be specific to the process
            formatted_xpaths = {k:v.format(internal_id=internal_id) for k,v in d.items()}
            extract_procceses = apply_xpaths_to_xml_file(formatted_xpaths,etree)

            # reorganise
            exchanges_dict[extract_procceses['flow_uuid']] = {
            'exchanges_name':extract_procceses['exchanges_name'],
            'exchanges_uuid':extract_procceses['exchanges_uuid'],
            'exchanges_resulting_amount':extract_procceses['exchanges_amount'],
            'exchanges_param_name':extract_procceses['exchanges_param_name'],
            "exchanges_amount_min":extract_procceses['exchanges_amount_min'],
            "exchanges_amount_max":extract_procceses['exchanges_amount_max'],
            "exchanges_amount_distrib":extract_procceses['exchanges_amount_distrib'],
            'exchanges_internal_id':internal_id,

            }
        ex.append(exchanges_dict)

    return ex
        

def get_param_data(etree_dict:dict)->list:
    """_summary_

    Parameters
    ----------
    etree_dict : dict
        _description_

    Returns
    -------
    list
        each element contains a list of parameters (if existing of each of the 
        activities in the ilcd zip file)
    """

    # get parameter names
    pnames_d = {}
    file_uuid_dict = {}

    for file,etree in etree_dict['processes'].items():
        pnames = apply_xpaths_to_xml_file(
            {'uuid':"/processDataSet/processInformation/dataSetInformation/common:UUID/text()",
            "parameter_name":"/processDataSet/processInformation/mathematicalRelations/variableParameter/@name"},
            etree)
        pnames_d[pnames['uuid']] = pnames['parameter_name']
        # file_uuid_dict[file] = pnames['uuid']
        
    # preformated xpaths    
    param_xpaths_unformatted = {
        "parameter_name":"/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/@name",
        "parameter_comment":"/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/comment/text()",
        "parameter_mean_value":"/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/meanValue/text()",
        # "parameter_mean_value":"/processDataSet/processInformation/mathematicalRelations/variableParameter/meanValue/text()",
        "parameter_minimum_value":"/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/minimumValue/text()",
        "parameter_maximum_value":"/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/maximumValue/text()",
        "parameter_std95":"/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/relativeStandardDeviation95In/text()",
        "parameter_formula":"/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/formula/text()",
        "parameter_distrib":"/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/uncertaintyDistributionType/text()",
        }
    
    # get 
    act_param_list = []
    for file,etree in etree_dict['processes'].items():
        uuid = Path(file).stem[0:36]
        
        parameters = pnames_d[uuid]

        if parameters is None:
            parameters = []

        act_param = []
        for p in parameters:

            formatted_xpaths = {k:v.format(parameter_name=p) for k,v in param_xpaths_unformatted.items()}
            param_data = apply_xpaths_to_xml_file(formatted_xpaths,etree)
            act_param.append(param_data)

        act_param_list.append(act_param)


    return act_param_list

def get_act_info(etree_dict:dict)->list:

    xpaths_activity_info = {
    "basename": "/processDataSet/processInformation/dataSetInformation/name/baseName/text()",
    "treatment_standards_routes": "/processDataSet/processInformation/dataSetInformation/name/treatmentStandardsRoutes/text()",
    "mix_and_location_types": "/processDataSet/processInformation/dataSetInformation/name/mixAndLocationTypes/text()",
    "functional_unit_flow_properties": "/processDataSet/processInformation/dataSetInformation/name/functionalUnitFlowProperties/text()",
    "uuid": "/processDataSet/processInformation/dataSetInformation/common:UUID/text()",
    "general_comment":"/processDataSet/processInformation/dataSetInformation/common:generalComment/text()",
    "reference_year": "/processDataSet/processInformation/time/common:referenceYear/text()",
    "data_set_valid_until": "/processDataSet/processInformation/time/common:dataSetValidUntil/text()",
    "time_representativeness_description": "/processDataSet/processInformation/time/common:timeRepresentativenessDescription/text()",
    "location": "/processDataSet/processInformation/geography/locationOfOperationSupplyOrProduction/@location",
    "LatLong": "/processDataSet/processInformation/geography/locationOfOperationSupplyOrProduction/@latitudeAndLongitude",
    "reference_to_reference_flow": "/processDataSet/processInformation/quantitativeReference/referenceToReferenceFlow/text()",
        }
        
    act_info_list = []
    for file,etree in etree_dict['processes'].items():
            act_info = apply_xpaths_to_xml_file(xpaths_activity_info,etree)
            act_info_list.append(act_info)

    act_param_list = get_param_data(etree_dict)

    for act,params in zip(act_info_list,act_param_list):

        act['parameters'] = params

    return act_info_list


def extract(path_to_zip)->list:

    etrees_dict = extract_zip(path_to_zip)


    #get product system model if exists
    if 'lifecyclemodels' in etrees_dict:
        psm = get_systemmodel(etrees_dict)

    # get contanct data
    contact_list = get_contact_from_etree(etrees_dict)

    # get unit group data 
    unit_gr_dict = get_unitdata_from_etree(etree_dict=etrees_dict)

    # get flow properties
    flow_properties_list = get_flowproperties_from_etree(etrees_dict)

    # combine the flow property and unit data
    unit_fp_dict = fp_dict(flow_properties=flow_properties_list, ug_dict=unit_gr_dict)

    # extract more info from `flows` folder
    flow_list = get_flows_from_etree(etrees_dict)

    # general data from activities
    act_info = get_act_info(etrees_dict)

    # exchange data from activities
    exchanges_list = get_exchange_data(etrees_dict)

    assert len(exchanges_list) == len(act_info)

    ## combine

    # add flow property and unit to flows
    for f in flow_list:
        f['flow property'] = unit_fp_dict[f['refobj']]['flow property']
        f['unit'] = unit_fp_dict[f['refobj']]['unit']
        f['unit_multiplier'] = unit_fp_dict[f['refobj']]['unit_multiplier']
        f['unit_group'] = unit_fp_dict[f['refobj']]['unit_group']

    flow_df = pd.DataFrame(flow_list)

    activity_info_list = []
    for act,exchanges in zip(act_info,exchanges_list):

        # put flow data into the exchanges (lazy approach using pandas)
        exchanges_df = pd.DataFrame(exchanges).T.merge(flow_df,
        left_on='exchanges_uuid',right_on='uuid',how="inner")

        # scale according to bizzare logic
        exchanges_df['amount'] = exchanges_df['exchanges_resulting_amount']\
        .map(float) * exchanges_df['value'].map(float)

        act["exchanges"] = exchanges_df.to_dict("records")
        act["contacts"] = contact_list
        act["flow properties"] = flow_properties_list

        act['_unit_flow_prop'] = unit_fp_dict # used later for unit conv

        activity_info_list.append(act)

    return activity_info_list    