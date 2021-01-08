
def show_tree(lst):
    last_dir = []
    for path in lst:
        items = [item for item in path.split("/") if item]
        if items and items[0] == '':
            items = items[1:]
        dir_path = items[:-1]
        filename = items[-1]
        n_common = 0
        for li, i in zip(dir_path, last_dir):
            if li == i:
                n_common += 1
            else:
                break
        if n_common == len(last_dir):
            tail = dir_path[n_common:]
            indent = 
        
        
        
        
        
        