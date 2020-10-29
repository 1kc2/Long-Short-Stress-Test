"""
    Util methods
"""

def stringify(obj, is_list=False, int_list=False):
    if is_list:
        if int_list:
            return "(" + ",".join([str(i) for i in obj]) + ")"
        else:
            return "(" + ",".join(['\''+str(i)+'\'' for i in obj]) + ")"
    else:
        return '\'' + str(obj) + '\''

