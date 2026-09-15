# podwatcher

## build & push image

```bash
./scripts/build_image.sh -t 0.0.2 -p
```

## deploy (Helm template + Kustomize)

```bash
# make targets
make deploy-dev
make deploy-uat
make deploy-prod

# diff before deploy
make diff-dev
make diff-uat
make diff-prod
```

actual commands:

```bash
# 1. helm template render
helm template podwatcher ./charts/podwatcher > ./kustomize/base/rendered.yaml

# 2. kubectl apply with overlay
kubectl apply -k ./kustomize/overlays/dev
kubectl apply -k ./kustomize/overlays/uat
kubectl apply -k ./kustomize/overlays/prod
```

## podwatcher deployment

```bash
kubectl rollout restart deployment podwatcher -n podwatcher
kubectl edit svc yunikorn-service -n yunikorn
```

## application discovery (incremental)

Downstream services discover applications with a timestamp cursor instead of
a start/end time window:

```bash
curl "ip:8080/api/v1/applications?since=2026-09-14T10:00:00Z&limit=100"
```

- `since`: RFC3339 timestamp of the last consumed change; omitted for the
  first call. Returns records whose `changedAt` is strictly after `since`,
  ordered by `changedAt` ascending.
- `limit`: page size, default 100, max 200; `hasMore=true` means another page
  exists. There is no `nextSince` field: persist the last record's
  `changedAt` from each page and pass it as the next `since`.
- optional `applicationId` and `status` filters are exact matches; `status`
  accepts a comma-separated list, e.g. `status=running,pending` (spaces around
  commas are ignored), and an empty value matches every status; terminal
  discovery uses `status=succeeded,failed,deleted`.
- response fields: `applications`, `count`, `hasMore`.
- `changedAt` advances only on lifecycle changes (new application, status
  change, finished/deleted); executor heartbeats only refresh
  `lastUpdatedAt`. Timestamps are strictly increasing with 1ms resolution;
  store `since` with at least millisecond precision.
- delivery is at-least-once; upsert by `applicationId` (a newer `changedAt`
  overwrites the older state).
- after a podwatcher restart all live applications are re-reported with fresh
  `changedAt` values (watch resume replays missed events, cold start re-lists
  the cluster); cluster nodes must be NTP-synced, otherwise a backward clock
  jump after rescheduling can make new changes older than a persisted since.
- deleted records are evicted past `maxApplications` (default 1000) and a
  resourceVersion older than the cluster watch-history window forces a full
  relist; a consumer whose `since` lags beyond either boundary may miss
  terminal states.

## resourceVersion checkpoint

The watch resourceVersion is checkpointed so a restart resumes the watch and
replays events missed during downtime instead of a cold full relist:

- `pods.checkpointUrl` (env `PODWATCHER_CHECKPOINT_URL`): base URL of the
  downstream checkpoint service. When empty, `pods.resumeFile` is used for
  local development; when both are empty resume is disabled.
- `pods.checkpointFlushSeconds` (env `PODWATCHER_CHECKPOINT_FLUSH_SECONDS`,
  default 10): maximum amount of watch progress lost on a hard crash; it must
  stay well below the cluster's watch-history (etcd compaction) window.
- checkpoint API contract (opaque JSON, no auth):
  - `GET {checkpointUrl}/checkpoints/podwatcher:resume:_all` -> 200 with
    `{"resourceVersion":"..."}`, or 404 when no checkpoint exists.
  - `PUT {checkpointUrl}/checkpoints/podwatcher:resume:_all` with the same
    JSON body and `Content-Type: application/json`.
- applications and pod records are not checkpointed externally; on disk-less
  deployments set `applicationsFile`/`podRecordsFile` to empty and keep the
  deployment at a single replica (checkpoint PUT is last-writer-wins).
## re-submit spark job

kubectl delete -f spark-pi.yml && sleep 1 && kubectl apply -f spark-pi.yml

## check

response of events `curl ip:8080/api/v1/events| jq .`: `setup/events_response.json

response of spark-applications `curl ip:8080/api/v1/spark-applications| jq .`: `setup/appications_response.json

pod log: `setup/pod.log`
