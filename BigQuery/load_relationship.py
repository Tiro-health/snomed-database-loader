`≥from typing import Literal
from dataclasses import dataclass, field
import re
import logging
from google.cloud import bigquery
from base_loader import BaseLoader
LOGGER = logging.getLogger(__name__)

@dataclass
class LoadRelationship(BaseLoader):
    """Load the SNOMED CT RF2 release Relationship Table into BigQuery"""
    client: bigquery.Client = field(init=False)
    dataset: str = field(default='snomed_ct')
    release_uri:str = field(default='gs://tiro-health-terminology/SnomedCT_ManagedServiceBE_PRODUCTION_BE1000172_20221115T120000Z')
    source_uris:list[str] = field(init=False)
    table:str = "Relationship"
    mode:Literal["append","write"] = field(default="write")

    @property
    def schema(self):
        return [
            bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("effectiveTime", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("active", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("moduleId", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("sourceId", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("destinationId", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("relationshipGroup", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("typeId", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("characteristicTypeId", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("modifierId", "INT64", mode="REQUIRED"),
        ]

    def __post_init__(self):
        # create bigquery client
        super().__post_init__()
        self.source_uris = [self.release_uri + f"/Full/Terminology/sct2_Relationship_Full_{self.version_tag}.txt"]