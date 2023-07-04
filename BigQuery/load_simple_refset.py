from dataclasses import dataclass, field
import re
import logging
from typing import Literal
from google.cloud import bigquery
from base_loader import BaseLoader
LOGGER = logging.getLogger(__name__)

@dataclass
class LoadSimpleRefset(BaseLoader):
    """Load the SNOMED CT RF2 release Relationship Table into BigQuery"""
    refsets: list[str] = field()
    directory: str = field()
    client:bigquery.Client = field(init=False)
    source_uris:list[str] = field(init=False)
    dataset:str = field(default='snomed_ct')
    release_uri:str = field(default='gs://tiro-health-terminology/SnomedCT_ManagedServiceBE_PRODUCTION_BE1000172_20221115T120000Z')
    source_uris:list[str] = field(init=False)
    table:str = "SimpleRefSet"
    mode:Literal["append","write"] = field(default="write")

    @property
    def schema(self):
        return [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("effectiveTime", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("active", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("moduleId", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("refsetId", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("referencedComponentId", "INT64", mode="REQUIRED"),
        ]

    def __post_init__(self):
        # create bigquery client
        super().__post_init__()
        self.source_uris = [self.release_uri + f"/Full/Refset/{self.directory}/der2_Refset_{refset}_{self.version_tag}.txt" for refset in self.refsets]