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


def merge_node(type, properties=None, **kwargs):
    placeholders = _placeholders(properties, kwargs)
    query = f"""
    MERGE (node:{type} {{{placeholders}}})
    RETURN node
    """
    return query


def merge_relationship(type, from_type, to_type, properties=None, directed=True, **kwargs):
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
