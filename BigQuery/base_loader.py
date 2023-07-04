from typing import TypeVar, Literal
import re
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging
from dataclasses import dataclass
LOGGER = logging.getLogger(__name__)

class BaseLoader:
    """Load the SNOMED CT RF2 release into BigQuery"""

    mode:Literal["append","write"]
    table:str
    release_uri:str
    dataset: str

    def __post_init__(self):
        # create bigquery client
        self.client = bigquery.Client()
        self.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if self.mode == "write" else bigquery.WriteDisposition.WRITE_APPEND
        version_suffix = re.findall(r"\w\w1000\d{3}_\d{8}T\d{6}Z",self.release_uri)[0]
        assert version_suffix is not None, "Could not find version suffix in release URI (=%s). (version_tag=%s)" % (self.release_uri,self.version_tag)
        self.version_tag = version_suffix.split("T")[0]

    @property
    def dataset_ref(self):
       return self.client.dataset(self.dataset)

    @property
    def schema(self)->list[bigquery.SchemaField] | None:
        return None

    @property
    def table_ref(self):
        return self.dataset_ref.table(self.table)

    def create_job_config(self)->bigquery.LoadJobConfig:

        # define BigQuery load job config
        job_config = bigquery.LoadJobConfig()
        if self.schema is not None:
            job_config.schema = self.schema
        job_config.autodetect = True
        job_config.skip_leading_rows = 1
        job_config.quote_character = ''
        job_config.field_delimiter = "\t"
        job_config.source_format = bigquery.SourceFormat.CSV,
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        return job_config

        # call BigQuery load job
    def run(self):
        load_job = self.client\
            .load_table_from_uri(
                source_uris=self.source_uris, 
                destination=self.table_ref,
                job_config=self.create_job_config(),
            )
        LOGGER.info("Starting job {}".format(load_job.job_id))

        # wait for load job to complete
        load_job.result()  # Waits for table load to complete.
        LOGGER.info("Job finished.")

    def complete(self):
        try:
            self.client.get_table(self.table_ref)
            return True
        except NotFound:
            return False

LoadJob = TypeVar("LoadJob", bound=BaseLoader)
def merge_jobs(*jobs:LoadJob)->LoadJob:
    """Merge source uri's of jobs into the first one"""

    first_job, *other_jobs = jobs
    for job in other_jobs:
        first_job.source_uris.extend(job.source_uris)
    return first_job
        