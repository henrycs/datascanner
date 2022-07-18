
def split_securities(all_secs_in_cache):
    all_secs = set()
    all_indexes = set()
    for sec in all_secs_in_cache:
        code = sec['code']
        _type = sec['type']
        if _type == "stock":
            all_secs.add(code)
        else:
            all_indexes.add(code)
    
    return all_secs, all_indexes