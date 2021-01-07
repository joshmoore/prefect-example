#!/usr/bin/env python

import pandas as pd
import pangeo_forge
import pangeo_forge.utils
import zarr
from pangeo_forge.tasks.http import download
from pangeo_forge.tasks.xarray import combine_and_write
from pangeo_forge.tasks.zarr import consolidate_metadata
from prefect import Flow, Parameter, task, unmapped, Client
from prefect.tasks.shell import ShellTask
from prefect.environments.execution.local import LocalEnvironment
from prefect.environments.storage.local import Local
from prefect.utilities.exceptions import (
    ClientError,
)


@task
def source_url(unused: str) -> str:
    return "https://downloads.openmicroscopy.org/images/DICOM/samples/US-MONO2-8-8x-execho.dcm"


@task
def build_command(cache: str, target: str):
    """
    Primary logic, requires:

        conda install -c ome bioformats2raw

    """
    cmd = f"bioformats2raw --file_type=zarr --dimension-order=XYZCT "
    cmd += f"{cache} {target}"
    return cmd


@task
def consolidate(shell_result, path: str) -> None:
    # TODO: check shell output
    zarr.consolidate_metadata(zarr.open(path).store)


shell = ShellTask(log_stdout=True)


class Pipeline(pangeo_forge.AbstractPipeline):
    name = "example"
    repo = "ome/pangeo-forge-example"

    cache_location = Parameter("cache_location", default=f"_tmp_download_{name}")
    target_location = Parameter("target_location", default=f"_tmp_convert_{name}")

    @property
    def sources(self):
        # This can be ignored for now.
        pass

    @property
    def targets(self):
        # This can be ignored for now.
        pass

    @property
    def flow(self):
        with Flow(self.name) as flow:
            url = source_url(None)  # TODO: Map all items from registry
            file = download(url, cache_location=self.cache_location)
            cmd = build_command(file, target=self.target_location)
            zarr = shell(command=cmd)
            consolidate(zarr, self.target_location)

        return flow


flow = Pipeline().flow
flow.environment = LocalEnvironment(labels=["all"])
flow.storage = Local()

if __name__ == "__main__":
    try:
        client = Client()
        client.create_project(project_name="example")  # Not idempotent!
    except ClientError:
        print("Failed to create project: example. May already exist")
    flow.register(project_name="example")
