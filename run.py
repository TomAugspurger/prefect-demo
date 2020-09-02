from elt_flow import flow


def get_executor():
    print("Creating executor")
    auth = JupyterHubAuth(os.environ["PANGEO_TOKEN"])
    # external
    gateway = Gateway(
        address="https://staging.us-central1-b.gcp.pangeo.io/services/dask-gateway/",
        auth=auth
    )
    cluster = gateway.new_cluster(shutdown_on_close=False)
    print(cluster.dashboard_link)
    cluster.adapt(0, 10)

    # Running on the cluster, use the internal address.
    parsed = urllib.parse.urlparse(cluster.scheduler_address)
    scheduler_address = urllib.parse.urlunparse(
        parsed._replace(netloc="traefik-gcp-uscentral1b-staging-dask-gateway.staging:80")
    )
    print(scheduler_address)

    executor = DaskExecutor(
        address=scheduler_address,
        client_kwargs={"security": cluster.security}
    )
    return executor


if __name__ == "__main__":
    flow.environment.executor = get_executor()
    flow.run()
