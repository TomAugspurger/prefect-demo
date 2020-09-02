1. Create token

```console
$ prefect auth create-token -n pangeo-forge-token --scope=RUNNER
<TOKEN>
```

2. Apply rbac

```console
$ kubectl -n staging apply -f prefect-rbac.yaml
role.rbac.authorization.k8s.io/prefect-agent-rbac configured
rolebinding.rbac.authorization.k8s.io/prefect-agent-rbac unchanged
```

3. Install prefect agent

```console
$ prefect agent install kubernetes -t <TOKEN> --rbac --namespace=staging --image-pull-policy=Always | kubectl apply -n staging -f -
deloyment.apps/prefect-agent configured
role.rbac.authorization.k8s.io/prefect-agent-rbac created
rolebinding.rbac.authorization.k8s.io/prefect-agent-rbac created
```

Note: I tried `--label=...` but that messed up prefect. Didn't run the flow.

4. Verify prefect sees the agent at https://cloud.prefect.io/tomaugspurger?agents=.

5. Add the flow

```coneole
$ python etl_flow.py 
/Users/taugspurger/miniconda3/envs/pangeo-forge-37/lib/python3.7/site-packages/prefect/client/client.py:693: UserWarning: No result handler was specified on your Flow. Cloud features such as input caching and resuming task runs from failure may not work properly.
  "No result handler was specified on your Flow. Cloud features such as "
Extracting [==================================================>]  62.45MB/62.45MBB
...
Successfully built 295fc36f08a9
Successfully tagged tomaugspurger/etl-flow:latest
[2020-09-01 19:38:43] INFO - prefect.Docker | Pushing image to the registry...
Pushing [==================================================>]  128.7MB/126.3MB
Flow: https://cloud.prefect.io/tomaugspurger/flow/b4089aec-cba5-43d2-9be0-d0f7af2fe33e
```

6. Run the flow

```console
$ prefect run flow --name=etl-flow --project=pangeo-forge
Flow Run: https://cloud.prefect.io/tomaugspurger/flow-run/619dcb30-b1b8-4eb6-818e-b3df0fc4980c
```
