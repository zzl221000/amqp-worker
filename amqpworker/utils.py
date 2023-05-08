import functools


def call(fn, *args, **kwargs):
    fn(*args, **kwargs)


def entrypoint(f):
    @functools.wraps(f)
    def _(*args, **kwargs):
        return f(*args, **kwargs)

    return _
