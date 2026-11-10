import inspect
from .storage import fingerprint
def identity(rows,params):return [dict(r) for r in rows]
def project(rows,params):return [{k:r[k] for k in params['fields']} for r in rows]
def sum_by(rows,params):
    key=params['key'];field=params['field'];totals={}
    for row in rows:totals[row[key]]=totals.get(row[key],0)+row[field]
    return [{key:k,field:v} for k,v in sorted(totals.items())]
REGISTRY={'identity':identity,'project':project,'sum_by':sum_by}
def transform_digest(name):
    if name not in REGISTRY:raise ValueError('变换未登记')
    return fingerprint({'name':name,'source':inspect.getsource(REGISTRY[name])})
def execute_transform(name,rows,params):
    if name not in REGISTRY:raise ValueError('变换未登记')
    return REGISTRY[name](rows,params)
