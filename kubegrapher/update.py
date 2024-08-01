from kubegrapher.cypher import set_k8snode_metrics

class K8sNodeMetrics():
    def __init__(self, cluser_id: str, hostname: str, metrics: dict[str: any]) -> None:
        self.metrics = metrics
        self.hostname = hostname
        self.cluster_id = cluser_id
    
    def set(self, tx: callable):
        query = set_k8snode_metrics(metrics=self.metrics)
        print('\n' + query + '\n')
        result = tx.run(query, self.metrics, hostname=self.hostname, cluster_id=self.cluster_id)
        return result.single()