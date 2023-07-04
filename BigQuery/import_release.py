import logging
import click
logging.basicConfig(level=logging.INFO)
from base_loader import merge_jobs
from load_concept import LoadConcept
from load_description import LoadDescription
from load_relationship import LoadRelationship
from load_simple_refset import LoadSimpleRefset
from load_language_refset import LoadLanguageRefset
from create_ancestors import CreateSnapRelationship,CreateSnapIsARelationship, CreateTransitiveClosure, CreateAncestors, CreateSnapDescription, CreateHSnapDescription, CreateInternationalFSNSnapDescription, CreateSnapConcept, CreateSnapLanguageRefSet
LOGGER = logging.getLogger(__name__)

@click.command()
@click.argument("bucket", type=str, default="")
@click.option("--force", is_flag=True, help="Force re-import of data")
@click.option("--be-release-folder", type=str, help="Foldername of the BE Release", required=True)
@click.option("--nl-release-folder", type=str, help="Foldername of the NL Release", required=True)
@click.option("--views-only", is_flag=True, help="Only create views")
def cli(bucket, be_release_folder, nl_release_folder,force:bool, views_only:bool):
    """Command line interface"""
    be_release_uri = f"gs://{bucket}/{be_release_folder}" 
    nl_release_uri = f"gs://{bucket}/{nl_release_folder}" 

    load_tasks = [
        merge_jobs(
            LoadConcept(release_uri=be_release_uri),
            LoadConcept(release_uri=nl_release_uri)
        ),
        merge_jobs(
            LoadDescription(release_uri=be_release_uri, languages=["nl", "fr", "en"]),
            LoadDescription(release_uri=nl_release_uri, languages=["nl", "en"]),
        ),
        merge_jobs(
            LoadRelationship(release_uri=be_release_uri), 
            LoadRelationship(release_uri=nl_release_uri),
        ),
        merge_jobs(
            LoadSimpleRefset(
                mode="write",
                release_uri=be_release_uri,
                directory="Content", 
                refsets=["GPSubsetSimpleRefsetFull"]
            ),
            LoadSimpleRefset(
                mode="append",
                release_uri=be_release_uri,
                directory="Content", 
                refsets=["MedicalProblemsInPatientHealthRecordsSimpleRefsetFull"]
            ),
            LoadSimpleRefset(
                mode="append",
                release_uri=be_release_uri,
                directory="Content", 
                refsets=["SubsetForAllergyIntoleranceCausativeAgentDrugSimpleRefsetFull"]
            )
        ),
        LoadLanguageRefset(
            mode="write",
            release_uri=be_release_uri,
            directory="Language", 
            refsets=[f"LanguageFull-{lang_code}" for lang_code in ["en", "fr-be-gp", "nl-be-gp", "fr", "nl"]]
        ),
    ]
    view_tasks = [
        CreateSnapConcept(),
        CreateSnapDescription(),
        CreateSnapLanguageRefSet(),
        CreateHSnapDescription(),
        CreateInternationalFSNSnapDescription(),
        CreateSnapRelationship(),
        CreateSnapIsARelationship(),
        CreateTransitiveClosure(),
        CreateAncestors(),
    ]
    tasks = view_tasks if views_only else load_tasks+view_tasks
    for task in tasks:
        LOGGER.info("Running %s" % task)
        task.run()
        if task.complete() and not force:
            LOGGER.info("Table %s exists already. Use --force to re-import." % task.table)
        else:
            task.run()
    
if __name__ == "__main__":
    cli()

