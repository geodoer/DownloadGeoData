import os

def get_all_file(root_path,all_files=[]):
    '''
    递归函数，遍历该文档目录和子目录下的所有文件，获取其path
    '''
    files = os.listdir(root_path)
    for file in files:
        if not os.path.isdir(root_path + '/' + file):   # not a dir
            all_files.append(root_path + '/' + file)
        else:  # is a dir
            get_all_file((root_path+'/'+file),all_files)
    return all_files

def get_dir(fp):
    return os.path.dirname(fp)

def get_ext(fp):
    return os.path.splitext(fp)[-1][1:]

def get_outfp(in_fp, out_dir, ext = None):
    if ext is None:
        ext = get_ext(in_fp)

    basename = os.path.basename(in_fp)
    fn = os.path.splitext(basename)[0]
    return os.path.join(out_dir, f"{fn}.{ext}")