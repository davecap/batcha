def split_path(path):
    split_path = path.split('/')
    leaf = split_path.pop()
    leaf_path = '/'.join(split_path)
    return (leaf_path, leaf)
