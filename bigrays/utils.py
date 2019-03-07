import io

import pandas as pd


def _public_attrs(obj):
    attrs = ((attr, getattr(obj, attr))
             for attr in dir(obj)
             if not attr.startswith('_')
             and not callable(getattr(obj, attr)))
    return sorted(attrs)


# must define this in utils rather than `mixins` to avoid ciruclar
# import with `resources`
class ReprMixin:
    def __repr__(self):
        name = getattr(self, '__name__', self.__class__.__name__)
        attrs = ', '.join(f'{attr}={val!r}' for attr, val in _public_attrs(self))
        return f'{name}({attrs})'


def _obj_to_byte_stream(obj):
    stream = io.BytesIO()
    if isinstance(obj, pd.DataFrame):
        stream.write(obj.to_csv(index=False).encode())
    elif isinstance(obj, str):
        stream.write(obj.encode())
    elif isinstance(obj, bytes):
        stream.write(obj)
    else:
        raise ValueError(f'unrecognized data type {type(obj)}')
    stream.seek(0)
    return stream
