# Script containing the mapping function for the schema-assurance mapper.

import os

import logging

import ies_tool.ies_tool as ies

from rdflib import Graph

from dsi_schema_assurance import SchemaCertifier

tool = ies.IESTool(mode="rdflib")

logger = logging.getLogger(__name__)

def schema_validation_trigger(item):

    tool.clear_graph()

    assessment_record = item

    tool.graph.parse(data=assessment_record, format="xml")

    shacl_path = os.path.join(os.getcwd(), 'validation_files', 'shacl.ttl')
    ontology_path = os.path.join(os.getcwd(), 'validation_files', 'ontology.rdf')
    
    shacl_graph = Graph()
    shacl_graph.parse(shacl_path, format="turtle")
    
    ont_graph = Graph()
    ont_graph.parse(ontology_path, format="xml")
    
    validation_results = SchemaCertifier(
        data_graph = tool.graph,
        shacl_graph = shacl_graph,
        ont_graph = ont_graph,
        inference_type = 'both'
    ).run()

    return validation_results
