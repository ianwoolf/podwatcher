# podwatcher

# render template (optional, for preview)
helm template podwatcher ./charts/podwatcher > rendered.yaml

# install or upgrade (idempotent, like kubectl apply)
helm upgrade --install podwatcher ./charts/podwatcher --namespace podwatcher --create-namespace

# uninstall
helm uninstall podwatcher --namespace podwatcher

kubectl rollout restart deployment podwatcher -n podwatcher

kubectl edit svc yunikorn-service -n yunikorn

# check

response of events `curl ip:8080/api/v1/events| jq .`: `setup/events_response.json

response of spark-applications `curl ip:8080/api/v1/spark-applications| jq .`: `setup/appications_response.json

pod log: `setup/pod.log`
