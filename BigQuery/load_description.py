from typing import Literal
from dataclasses import dataclass, field
import re
import logging
from base_loader import BaseLoader
from google.cloud import bigquery

LOGGER = logging.getLogger(__name__)

@dataclass
class LoadDescription(BaseLoader):
    """Load the SNOMED CT RF2 release Description Table into BigQuery"""
    release_uri:str
    dataset: str = field(default='snomed_ct')
    languages:list[str] = field(default_factory=lambda: ["en","nl","fr"])
    source_uris:list[str] = field(init=False)
    client:bigquery.Client = field(init=False)
    table:str = "Description"
    mode:Literal["append","write"] = field(default="write")

    @property
    def schema(self):
        return [
            bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("effectiveTime", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("active", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("moduleId", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("conceptId", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("languageCode", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("typeId", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("term", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("caseSignificanceId", "INT64", mode="REQUIRED"),
        ]

    def __post_init__(self):
        # create bigquery client
        super().__post_init__()
        self.source_uris = [self.release_uri + f"/Full/Terminology/sct2_Description_Full-{lang}_{self.version_tag}.txt" for lang in self.languages]