
def delete_orphans(tx, type):
    query = f"""
        MATCH (n:{type})
        WHERE (NOT (n)--())
        DELETE (n)
        """
    result = tx.run(query)
    summary = result.consume()
    return summary.counters.nodes_deleted