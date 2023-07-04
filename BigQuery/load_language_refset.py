from dataclasses import dataclass, field
import re
import logging
from typing import Literal
from google.cloud import bigquery
from base_loader import BaseLoader
LOGGER = logging.getLogger(__name__)

@dataclass
class LoadLanguageRefset(BaseLoader):
    """Load the SNOMED CT RF2 release Relationship Table into BigQuery"""
    refsets: list[str] = field()
    directory: str = field()
    client:bigquery.Client = field(init=False)
    source_uris:list[str] = field(init=False)
    dataset:str = field(default='snomed_ct')
    release_uri:str = field(default='gs://tiro-health-terminology/SnomedCT_ManagedServiceBE_PRODUCTION_BE1000172_20221115T120000Z')
    source_uris:list[str] = field(init=False)
    table:str = "LanguageRefSet"
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
            bigquery.SchemaField("acceptabilityId", "INT64", mode="REQUIRED"),
        ]

    def __post_init__(self):
        # create bigquery client
        super().__post_init__()
        version_suffix = re.findall("BE1000172_\d{8}T\d{6}Z",self.release_uri)[0]
        assert version_suffix is not None, "Could not find version suffix in release URI (=%s). (version_tag=%s)" % (self.release_uri,version_tag)
        version_tag = version_suffix.split("T")[0]
        self.source_uris = [self.release_uri + f"/Full/Refset/{self.directory}/der2_cRefset_{refset}_{version_tag}.txt" for refset in self.refsets]