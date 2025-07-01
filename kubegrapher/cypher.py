from kubegrapher.relations import Relations

def _placeholders(properties=None, kwargs=None):
    properties_placeholders = None
    kwargs_placeholders = None
    if properties is not None:
        properties_placeholders = ', '.join(
            [f'{key}: ${key}' for key in properties.keys()])
    if kwargs is not None:
        kwargs_placeholders = ', '.join(
            [f'{key}: ${key}' for key in kwargs.keys()])

    if properties_placeholders and kwargs_placeholders:
        return f'{properties_placeholders}, {kwargs_placeholders}'
    elif properties_placeholders:
        return properties_placeholders
    elif kwargs_placeholders:
        return kwargs_placeholders
    else:
        return ''
    
def _to_placeholders(properties):
    return ', '.join(
        [f'{key}: $to_{key}' for key in properties.keys()])

def to_properties(properties):
    return {f'to_{key}': value for key, value in properties.items()}

def merge_node(type, properties=None, **kwargs):
    placeholders = _placeholders(properties, kwargs)
    query = f"""
    MERGE (node:{type} {{{placeholders}}})
    RETURN node
    """
    return query

def delete_node_query(type, **kwargs):
    placeholders = _placeholders(kwargs=kwargs)
    query = f"""
    MATCH (node:{type} {{{placeholders}}})
    DETACH DELETE node
    """
    return query

def delete_pod_query(**kwargs):
    placeholders = _placeholders(kwargs=kwargs)
    query = f"""
    MATCH (pod:{'Pod'} {{{placeholders}}})--(containers:{'Container'})
    DETACH DELETE containers, pod
    """
    return query


def merge_relationship(type: str, from_type: str, to_type: str, properties: dict[str: any] = None, directed = True, **kwargs):
    placeholders = _placeholders(properties=properties, kwargs=kwargs)
    if directed:
        arrow = '>'
    else:
        arrow = ''
    query = f"""
    MATCH (n0:{from_type} {{id: $from_id}}), (n1:{to_type} {{id: $to_id}})
    MERGE p = (n0)-[relation:{type} {{{placeholders}}}]-{arrow}(n1)
    RETURN p
    """
    return query


def merge_relationship_generic(type: str, from_type: str, to_type: str, to_properties: dict[str: any], properties: dict[str: any] = None, directed = True, **kwargs):
    placeholders = _placeholders(properties=properties, kwargs=kwargs)
    to_placeholders = _to_placeholders(to_properties)
    if directed:
        arrow = '>'
    else:
        arrow = ''
    query = f"""
    MATCH (n0:{from_type} {{id: $from_id}}), (n1:{to_type} {{{to_placeholders}}})
    MERGE p = (n0)-[relation:{type} {{{placeholders}}}]-{arrow}(n1)
    RETURN p
    """

    return query

def merge_relationship_service_to_pod():
    query = f"""
    MATCH (s:Service {{id: $service_id}}) -[:{Relations.HAS_SELECTOR}]-> (l:Label)
    WITH s, collect(l) AS selectorLst
    MATCH (p:Pod)
    WHERE ALL (label IN selectorLst WHERE EXISTS((p) -[:{Relations.HAS_LABEL}]-> (label)))
    MERGE (s) -[r:{Relations.EXPOSES}]-> (p)
    """
    return query

def merge_relationship_pod_to_service():
    query = f"""
    MATCH (p:Pod {{id: $pod_id}}) -[:{Relations.HAS_LABEL}]-> (l:Label)
    WITH p, collect(l) AS labelLst
    MATCH (s:Service)
    WHERE ALL (selector IN labelLst WHERE EXISTS((s) -[:{Relations.HAS_SELECTOR}]-> (selector)))
    MERGE (s) -[r:{Relations.EXPOSES}]-> (p)
    """
    return query

def merge_relationship_ingress_to_service():
    query = f"""
    MATCH (i:Ingress {{id: $ingress_id, cluster_id: $cluster_id}})
    WITH i
    MATCH (s:Service {{name: $service_name, cluster_id: $cluster_id}})
    MERGE (i) -[r:{Relations.ROUTES_TO}]-> (s)
    """
    return query

def merge_relationship_service_to_ingress():
    query = f"""
    MATCH (s:Service {{name: $service_name, cluster_id: $cluster_id}})
    WITH s
    MATCH (i:Ingress {{cluster_id: $cluster_id}})
    WHERE s.name IN i.service_names
    MERGE (i) -[r:{Relations.ROUTES_TO}]-> (s)
    """
    return query

def set_k8snode_metrics(metrics: dict[str: any]):
    placeholders = _placeholders(metrics)
    query = f"""
    MATCH (n:K8sNode {{hostname: $hostname}}) -[{Relations.BELONGS_TO}]-> (c:Cluster)
    WHERE c.id = $cluster_id
    SET n += {{{placeholders}}}
    RETURN n
    """
    return query

    # https://neo4j.com/docs/cypher-manual/current/clauses/set/#set-setting-properties-using-map