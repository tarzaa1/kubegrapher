CREATE (c1:Cluster)
CREATE (m1:K8sNode:ControlPlane {name: "clever-tarek-m1", creationTimestamp: "2023-09-20T10:06:47Z", ...})
CREATE (label:Label {key: "kubernetes.io/arch", value: "amd64"})
CREATE (annotation:Annotation {key: "flannel.alpha.coreos.com/backend-type", value: "vxlan"})
CREATE (taint:Taint {key: "node-role.kubernetes.io/control-plane", effect: "NoSchedule"})
CREATE (w1:K8sNode:Worker {name:'', ...})
CREATE
(c1)-[:CONTAINS_NODE]->(m1),
(c1)-[:CONTAINS_NODE]->(w1),
(m1)-[:HAS_WORKER]->(w1),
(w1)-[:HAS_CONTROL_PLANE]->(m1),
(m1)-[:HAS_LABEL]->(label),
(m1)-[:HASH_ANNOATATION]->(annoation),
(m1)-[:HAS_TAINT]->(taint)