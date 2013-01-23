import os


import re

path_to_original = 'data/john-smith/original'
path_to_new = 'data/john-smith/original-fixed'

## iterate over the files in the 35 input directories
for label_id in range(35):

    i_dir_path = os.path.join(path_to_original, str(label_id))
    o_dir_path = os.path.join(path_to_new, str(label_id))

    if not os.path.exists(o_dir_path):
        os.makedirs(o_dir_path)

    fnames = os.listdir(i_dir_path)
    fnames.sort()
    for fname in fnames:

        print fname

        i_path = os.path.join(i_dir_path, fname)
        o_path = os.path.join(o_dir_path, fname)
        raw_string = open(i_path).read()
        new_string = re.sub('\r', '\n', raw_string)

        if raw_string != new_string:
            print 'fixing %s' % i_path

        spaced_new_string = ''
        for line in new_string.splitlines():
            if not line.strip():
                continue
            line = line.rstrip() + ' \n'
            spaced_new_string += line

        open(o_path, 'wb').write(spaced_new_string)
