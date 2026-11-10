from datapack.domain import execute_transform,transform_digest
def check(case):
    rows=[{'city':'e','distance':1},{'city':'e','distance':2}]
    case.assertEqual(execute_transform('sum_by',rows,{'key':'city','field':'distance'}),[{'city':'e','distance':3}])
    result=execute_transform('identity',rows,{});result[0]['distance']=9;case.assertEqual(rows[0]['distance'],1)
    case.assertEqual(len(transform_digest('sum_by')),64)
    with case.assertRaises(ValueError):execute_transform('missing',rows,{})
