"""Read-only cloud preflight, no data query execution and no credential output."""
from google.auth import default
from google.auth.transport.requests import Request
from google.cloud import bigquery
from vmforecast.settings import Settings
from vmforecast.google2019 import ExtractionConfig, build_queries


def main():
    cfg = Settings.load()
    try:
        credentials, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        credentials.refresh(Request())
        client = bigquery.Client(project=cfg.project, credentials=credentials)
        ds = client.get_dataset(bigquery.DatasetReference(cfg.source_project, 'clusterdata_2019_a'))
        params = [bigquery.ScalarQueryParameter('t0', 'INT64', 600_000_000),
                  bigquery.ScalarQueryParameter('prefix_end', 'INT64', 4_200_000_000),
                  bigquery.ScalarQueryParameter('n', 'INT64', 20)]
        plan = client.query(build_queries(ExtractionConfig(source_project=cfg.source_project))['cohort'],
                            location=ds.location,
                            job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False,
                                                              query_parameters=params))
        print('ADC and BigQuery dry run OK; cohort estimated bytes:', plan.total_bytes_processed)
    except Exception as error:
        # Provider error messages may include identifiers; avoid dumping credential-related responses.
        print('Cloud preflight failed ('+type(error).__name__+'). Run sh lab auth; check project access and '
              'BigQuery Job User permissions. Keep the project in Sandbox; do not enable billing.')
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
