dict = [
    {
    'A': {
        'a': 1,
        'b': 2,
    },
    'basicdata': {
        'a': 3,
        'b': 4,
    }
    },
{
    'B': {
        'c': 3,
        'd': 4,
    },
    'basicdata': {
        'a': 5,
        'b': 6,
    }
},
{
    'C': {
        'e': 5,
        'f': 6,
    },
    'basicdata': {
        'a': 7,
        'b': 8,
    }
}]

for item in dict:
    parent_dict = item.copy()
    print("parent_dict >>>", parent_dict)

    basicdata = parent_dict.pop('basicdata')
    print("basicdata >>>", basicdata)