from typing import Literal
from dataclasses import field
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging
from dataclasses import dataclass
LOGGER = logging.getLogger(__name__)

class BaseTableCreator:
    """Load the SNOMED CT RF2 release into BigQuery"""
    table:str
    dataset:str = "snomed_ct"
    mode:Literal["append","write"] = "write"
    deps:list = field(default_factory=list)

    def __post_init__(self):
        self.client = bigquery.Client()
        self.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if self.mode == "write" else bigquery.WriteDisposition.WRITE_APPEND

    @property
    def dataset_ref(self)->bigquery.DatasetReference:
       return self.client.dataset(self.dataset)

    @property
    def table_ref(self):
        assert self.table is not None, "Please set the table property"
        return self.dataset_ref.table(self.table)

    def unfold_deps(self):
        for dep in self.deps:
            yield from dep.unfold_deps()
            yield dep

    def create_job_config(self)->bigquery.QueryJobConfig:

        # define BigQuery load job config
        job_config = bigquery.QueryJobConfig()
        job_config.destination = self.table_ref
        job_config.write_disposition = self.write_disposition 
        return job_config

    @property
    def sql(self)->str:
        raise NotImplementedError("Please implement the sql property")

        # call BigQuery load job
    def run(self):
        load_job = self.client\
            .query(
                query=self.sql,
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