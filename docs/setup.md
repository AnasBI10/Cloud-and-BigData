cat > ~/bigdata/docs/setup.md << 'EOF'
# Setup: Kubernetes-Cluster auf der DHBW 4C-Cloud

Story 24 | P6 | Stand: 01.09.2026

## Umgebung

DHBW 4C-Cloud (OpenStack), Projekt ma_wdski24b_cloud_computing.
Zugang über VPN (Cisco Secure Client) oder DHBW-Netz.

Da OpenStack nur VMs bereitstellt, wurde Kubernetes selbst installiert.
Gewählt: k3s - Installation in einem Befehl, vollwertiges Kubernetes,
geringer Overhead, StorageClass und Metrics-Server bereits enthalten.

## Instanz

| Feld | Wert |
|---|---|
| Name | ny-traffic-master |
| Abbild | Ubuntu Server 24.04 LTS |
| Variante | m1.extra_large (4 vCPU) |
| Interne IP | 192.168.10.53 |

SSH-Key vorher in OpenStack unter Compute -> Schluesselpaare hinterlegen.

## k3s installieren

    curl -sfL https://get.k3s.io | sh -
    sudo k3s kubectl get nodes

Version: k3s v1.36.4+k3s1

Join-Token fuer spaetere Worker:

    sudo cat /var/lib/rancher/k3s/server/node-token

## Namespace und Ressourcengrenzen

    sudo k3s kubectl create namespace bigdata

quota.yaml:

    apiVersion: v1
    kind: ResourceQuota
    metadata:
      name: bigdata-quota
      namespace: bigdata
    spec:
      hard:
        requests.cpu: "3"
        requests.memory: 6Gi
        persistentvolumeclaims: "6"

    sudo k3s kubectl apply -f quota.yaml

Begruendung: Der Node hat 4 vCPU. Die k3s-Systemkomponenten belegen davon
einen Teil. Die Quota begrenzt den Anwendungs-Namespace auf 3 vCPU und laesst
dem System Reserve, damit Anwendungs-Pods die Steuerungsebene nicht verdraengen.

## Storage

    sudo k3s kubectl get sc

local-path (rancher.io/local-path), als Default markiert. Wird im Helm-Chart
als global.storageClass gesetzt - parametrisiert, weil ein lokales
kind-Cluster stattdessen standard nutzt.

## Einschraenkung: Ressourcen-Kontingent

Das Projekt wird von mehreren Gruppen genutzt; beim Setup liefen dort bereits
12 Instanzen. Nach dem Start der Control-Plane war das vCPU-Kontingent
ausgeschoepft, Worker-Nodes konnten zunaechst nicht angelegt werden. Das
Cluster laeuft aktuell als Single-Node-Installation.

Deployments, StatefulSets, PVCs, ConfigMaps und Services funktionieren
uneingeschraenkt. Nicht demonstrierbar ist die Verteilung von Pods ueber
mehrere Nodes beim Skalieren.

Massnahmen: Quota-Erhoehung beim 4C-Team angefragt, Abstimmung im Kurs ueber
Freigabe alter Instanzen. Sobald Kontingent frei ist, genuegt auf jeder neuen
Instanz ein Befehl - bestehende Pods bleiben unveraendert:

    curl -sfL https://get.k3s.io | \
      K3S_URL=https://192.168.10.53:6443 K3S_TOKEN=<TOKEN> sh -

## Verifikation

    sudo k3s kubectl get nodes
    sudo k3s kubectl get resourcequota -n bigdata
    sudo k3s kubectl get sc
    sudo k3s kubectl get pods -A

## Versionen

Ubuntu Server 24.04 LTS | k3s v1.36.4+k3s1 | m1.extra_large (4 vCPU) |
StorageClass local-path | OpenStack, DHBW 4C-Cloud
EOF
